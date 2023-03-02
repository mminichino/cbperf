##
##

import logging
import sys
from enum import Enum
import pandas as pd
import json
import concurrent.futures
from lib.exceptions import ExportException, ExportError
from cbcmgr.cb_connect import CBConnect
from cbcmgr.cb_management import CBManager
import lib.config as config
from lib.main import MainLoop
from lib.schema import ProcessSchema
from lib.exec_step import DBRead, DBQuery


class ExportType(Enum):
    csv = 0
    json = 1


class CBExport(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

        try:
            self.db = CBConnect(config.host, config.username, config.password, ssl=config.tls).connect()
        except Exception as err:
            raise ExportException(f"can not connect to Couchbase: {err}")

        if not config.schema_name:
            self.import_schema()

    @staticmethod
    def import_schema():
        dbm = CBManager(config.host, config.username, config.password, ssl=config.tls)
        inventory = dbm.cluster_schema_dump()
        config.inventory = ProcessSchema(json_data=inventory).inventory()
        config.schema = config.inventory.get(config.bucket_name)

    def export(self, mode: ExportType):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.batch_size)
        run_batch_size = config.batch_size * 10

        for bucket in config.schema.buckets:
            self.db.bucket(bucket.name)

            for scope in bucket.scopes:
                if config.scope_name and config.scope_name != scope.name:
                    continue
                self.db.scope(scope.name)

                for collection in scope.collections:
                    data = []
                    tasks = set()

                    if config.collection_name and config.collection_name != collection.name:
                        continue

                    self.db.collection(collection.name)

                    if not self.db.has_primary_index(create=config.create_indexes):
                        raise ExportError("Primary index is required for export")

                    operation_count = self.db.collection_count()
                    if operation_count == 0:
                        break

                    query = r"select meta().id from {{ keyspace }} ;"
                    query_op = DBQuery(self.db, query, keyspace=self.db.keyspace)
                    query_op.execute()

                    self.logger.info(f"Processing collection {self.db.keyspace}")
                    output_file = f"{config.output_dir}/{str(self.db.keyspace).replace('.','-')}.{mode.name}"

                    db_op = DBRead(self.db, add_key=True)
                    doc_id_list = query_op.result

                    for n in range(1, len(doc_id_list) + 1, run_batch_size):
                        tasks.clear()
                        for b in range(n, n + run_batch_size):
                            if b > len(doc_id_list):
                                break
                            tasks.add(executor.submit(db_op.fetch, doc_id_list[b-1]['id']))
                        results = MainLoop().task_wait(tasks)
                        data.extend(results)

                    if self.db.has_primary_index() and config.create_indexes:
                        self.db.revert_primary_index()

                    self.logger.info(f" == Retrieved {operation_count} records")
                    self.logger.info(f" == Creating {output_file}")

                    if mode == ExportType.csv:
                        df = pd.json_normalize(data)
                        df.to_csv(output_file, encoding='utf-8', index=False)
                    elif mode == ExportType.json:
                        open(output_file, 'w').close()
                        write_file = open(output_file, 'a') if not config.screen_output else sys.stdout
                        for element in data:
                            block = json.dumps(element)
                            write_file.write(block + '\n')
