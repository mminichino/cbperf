#
#

import logging
import json
import concurrent.futures
import lib.config as config
from lib.config import OperatingMode
from cbcmgr.cb_connect import CBConnect
from cbcmgr.cb_management import CBManager
from lib.exceptions import TestRunError
from lib.exec_step import DBRead, DBWrite, DBQuery
from lib.mptools import MPValue
from lib.schema import Bucket, Scope, Collection


class MainLoop(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

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
        dbm = CBManager(config.host, config.username, config.password, ssl=config.tls).connect()
        dbm.create_bucket(bucket.name)
        dbm.create_scope(scope.name)
        dbm.create_collection(collection.name)

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
        i = MPValue()

        try:
            db = CBConnect(config.host, config.username, config.password, ssl=config.tls).connect(bucket.name, scope.name, collection.name)
        except Exception as err:
            raise TestRunError(f"can not connect to Couchbase: {err}")

        if collection.override_count:
            operation_count = collection.override_count
        else:
            operation_count = config.count

        db_op = DBWrite(db, collection.schema, collection.idkey)
        self.logger.info(f"Inserting {operation_count} records into collection {collection.name}")

        while True:
            tasks = set()
            for n in range(config.batch_size):
                if i.value >= operation_count:
                    break
                if config.op_mode == OperatingMode.LOAD.value:
                    tasks.add(executor.submit(db_op.execute, i.next))
            if len(tasks) == 0:
                break
            task_count = len(tasks)
            while tasks:
                done, tasks = concurrent.futures.wait(tasks, return_when=concurrent.futures.FIRST_COMPLETED)
                for task in done:
                    try:
                        result = task.result()
                    except Exception as err:
                        self.logger.error(f"task error: {err}")

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

    def read(self):
        bucket = config.bucket_name
        scope = config.scope_name
        collection = config.collection_name

        try:
            db = CBConnect(config.host, config.username, config.password, ssl=config.tls).connect(bucket, scope, collection)
        except Exception as err:
            raise TestRunError(f"can not connect to Couchbase: {err}")

        if config.document_key:
            db_op = DBRead(db, config.document_key)
            db_op.execute()
            try:
                output = json.dumps(db_op.result, indent=2)
            except json.decoder.JSONDecodeError:
                output = db_op.result
            print(output)
