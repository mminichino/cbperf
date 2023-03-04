#
#

import logging
import json
import re
import sys
import io
import itertools as it
import concurrent.futures
from functools import partial
import lib.config as config
import lib.randomize as rand
from cbcmgr.cb_connect import CBConnect
from cbcmgr.cb_management import CBManager
from lib.exceptions import TestRunError
from lib.exec_step import DBRead, DBWrite, DBQuery
from lib.schema import Bucket, Scope, Collection
from lib.schema import ProcessSchema


class MainLoop(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        rand.rand_init()

    @staticmethod
    def prep_bucket(bucket, scope, collection, quota: int = 256):
        dbm = CBManager(config.host, config.username, config.password, ssl=config.tls).connect()
        dbm.create_bucket(bucket, quota)
        dbm.create_scope(scope)
        dbm.create_collection(collection)
        return dbm

    def task_wait(self, tasks):
        result_set = []
        while tasks:
            done, tasks = concurrent.futures.wait(tasks, return_when=concurrent.futures.FIRST_COMPLETED)
            for task in done:
                try:
                    result = task.result()
                    if type(result) == dict:
                        result_set.append(result)
                except Exception as err:
                    self.logger.error(f"task error: {type(err).__name__}: {err}")
                    return
        return result_set

    def schema_remove(self):
        dbm = CBManager(config.host, config.username, config.password, ssl=config.tls).connect()
        if config.schema_name:
            bucket_list = [b.name for b in config.schema.buckets]
        else:
            bucket_list = [config.bucket_name]
        for bucket in bucket_list:
            if bucket:
                self.logger.info(f"Removing bucket {bucket}")
                dbm.drop_bucket(bucket)

    @staticmethod
    def schema_list():
        config.inventory = ProcessSchema(config.schema_file).inventory()
        for schema in config.inventory.inventory:
            print(f"Schema: {schema.name}")
            for bucket in schema.buckets:
                print(f"  Bucket: {bucket.name}")
                for scope in bucket.scopes:
                    print(f"    - Scope: {scope.name}")
                    for collection in scope.collections:
                        print(f"      > Collection: {collection.name}")
                        print(f"        Schema:")
                        json_output = json.dumps(collection.schema, indent=2)
                        lines = json_output.split('\n')
                        for line in lines:
                            print(f"               {line}")
            for rule in schema.rules:
                if rule.type == "link":
                    print(f"Rule: {rule.name}")
                    print(f"  Type: {rule.type}")
                    print(f"  SQL : {rule.sql}")

    def cluster_list(self):
        db = CBManager(config.host, config.username, config.password, ssl=config.tls)

        if config.wait_mode:
            try:
                db.wait_for_query_ready()
                db.wait_for_index_ready()
            except Exception as err:
                self.logger.error(f"cluster wait failed: {err}")
                raise TestRunError("cluster not ready")

        db.print_host_map()

        if config.ping_mode:
            if config.test_mode:
                db.cluster_health_check(output=False, restrict=False, extended=True)
            else:
                print("Cluster Status:")
                db.cluster_health_check(output=True, restrict=False)

    def schema_load(self):
        self.logger.info("Processing buckets")
        for bucket in config.schema.buckets:
            for scope in bucket.scopes:
                for collection in scope.collections:
                    self.logger.info(f"Processing bucket {bucket.name} scope {scope.name} collection {collection.name}")
                    self.pre_process(bucket, scope, collection)
                    self.process(bucket, scope, collection)
                    self.post_process(bucket, scope, collection)
        self.logger.info("Processing rules")
        for rule in config.schema.rules:
            if rule.type == "link":
                self.logger.info(f"Running link rule {rule.name}")
                self.run_link_rule(rule.id_field, rule.primary_key, rule.foreign_key)

    def pre_process(self, bucket: Bucket, scope: Scope, collection: Collection):
        self.logger.info("Creating bucket structure")
        dbm = self.prep_bucket(bucket.name, scope.name, collection.name)

        self.logger.info("Processing indexes")
        if collection.primary_index:
            dbm.cb_create_primary_index(replica=config.replicas)
            self.logger.info(f"Created primary index on {collection.name}")
        if collection.indexes:
            index_name = dbm.cb_create_index(fields=collection.indexes, replica=config.replicas)
            collection.set_index_name(index_name)
            self.logger.info(f"Created index {index_name} on {','.join(collection.indexes)}")

    def process(self, bucket: Bucket, scope: Scope, collection: Collection):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.batch_size)
        run_batch_size = config.batch_size * 10
        tasks = set()
        rand.prepare_template(collection.schema)

        try:
            db = CBConnect(config.host, config.username, config.password, ssl=config.tls).connect(bucket.name, scope.name, collection.name)
        except Exception as err:
            raise TestRunError(f"can not connect to Couchbase: {err}")

        if collection.override_count:
            operation_count = collection.override_count
        else:
            operation_count = config.count

        db_op = DBWrite(db, collection.idkey)
        self.logger.info(f"Inserting {operation_count} records into collection {collection.name}")

        for n in range(1, operation_count + 1, run_batch_size):
            tasks.clear()
            for key in range(n, n + run_batch_size):
                if key > operation_count:
                    break
                document = rand.process_template()
                tasks.add(executor.submit(db_op.execute, key, document))
            self.task_wait(tasks)

    def post_process(self, bucket: Bucket, scope: Scope, collection: Collection):
        pass

    def run_link_rule(self, id_field: str, source_keyspace: str, target_keyspace: str):
        s_keyspace = '.'.join(source_keyspace.split(':')[:3])
        t_keyspace = '.'.join(target_keyspace.split(':')[:3])
        t_field = target_keyspace.split(':')[-1]

        try:
            db = CBConnect(config.host, config.username, config.password, ssl=config.tls).connect()
        except Exception as err:
            raise TestRunError(f"can not connect to Couchbase: {err}")

        query = f"MERGE INTO {t_keyspace} t USING {s_keyspace} s ON t.{id_field} = s.{id_field} WHEN MATCHED THEN UPDATE SET t.{t_field} = meta(s).id ;"
        self.logger.debug(f"running rule query {query}")
        db_op = DBQuery(db, query)
        db_op.execute()

    def input_load(self):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.batch_size)
        decoder = json.JSONDecoder()
        bucket = config.bucket_name
        scope = config.scope_name
        collection = config.collection_name
        tasks = set()

        self.logger.info(f"Inserting records into collection {collection}")

        try:
            self.prep_bucket(bucket, scope, collection)
            db = CBConnect(config.host, config.username, config.password, ssl=config.tls).connect(bucket, scope, collection)
        except Exception as err:
            raise TestRunError(f"can not connect to Couchbase: {err}")

        if config.insert_data:
            content = io.StringIO(config.insert_data)
        else:
            content = sys.stdin

        count = db.collection_count()

        object_count = 0
        key_count = count
        buffer = ''
        for chunk in iter(partial(content.read, 131072), ''):
            tasks.clear()
            buffer += chunk
            while buffer:
                try:
                    json_object, position = decoder.raw_decode(buffer)
                    db_op = DBWrite(db)
                    key_count += 1
                    if config.key_field in json_object:
                        doc_key = json_object[config.key_field]
                    else:
                        doc_key = key_count
                    tasks.add(executor.submit(db_op.execute, doc_key, json_object))
                    object_count += 1
                    buffer = buffer[position:]
                    buffer = buffer.lstrip()
                except ValueError:
                    break
            self.task_wait(tasks)

        self.logger.info(f"Collection had {count} documents - inserted {object_count} additional record(s)")

    def read(self):
        bucket = config.bucket_name
        scope = config.scope_name
        collection = config.collection_name

        try:
            db = CBConnect(config.host, config.username, config.password, ssl=config.tls).connect(bucket, scope, collection)
        except Exception as err:
            raise TestRunError(f"can not connect to Couchbase: {err}")

        if config.document_key:
            self.read_by_key(config.document_key, db)
        else:
            self.read_by_meta_id(db)

    @staticmethod
    def read_by_key(key: str, db: CBConnect, start: int = 1):
        count = it.count(start)
        db_op = DBRead(db)

        while True:
            lookup_key, n = re.subn(r"%N", lambda x: str(next(count)), key)
            db_op.execute(lookup_key)
            if not db_op.result:
                break
            try:
                output = json.dumps(db_op.result, indent=2)
            except json.decoder.JSONDecodeError:
                output = db_op.result
            print(output)
            if n == 0:
                break

    @staticmethod
    def read_by_meta_id(db: CBConnect):
        query = r"select meta().id from {{ keyspace }} ;"
        query_op = DBQuery(db, query, keyspace=db.keyspace)
        query_op.execute()
        db_op = DBRead(db)
        for meta_id in query_op.result:
            db_op.execute(meta_id['id'])
            try:
                output = json.dumps(db_op.result, indent=2)
            except json.decoder.JSONDecodeError:
                output = db_op.result
            print(output)
