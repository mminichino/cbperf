##
##

import logging
import pandas as pd
import concurrent.futures
from lib.exceptions import ExportException
from cbcmgr.cb_connect import CBConnect
from cbcmgr.cb_management import CBManager
import lib.config as config
from lib.main import MainLoop
from lib.schema import ProcessSchema


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

    def as_csv(self):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.batch_size)
        run_batch_size = config.batch_size * 10

        for bucket in config.schema.buckets:
            self.db.bucket(bucket.name)

            for scope in bucket.scopes:
                self.db.scope(scope.name)

                for collection in scope.collections:
                    data = []
                    tasks = set()

                    self.db.collection(collection.name)
                    operation_count = self.db.collection_count()
                    if operation_count == 0:
                        break
                    print(f"Processing collection {self.db.keyspace}")
                    output_file = f"{config.output_dir}/{str(self.db.keyspace).replace('.','-')}.csv"

                    for n in range(1, operation_count + 1, run_batch_size):
                        tasks.clear()
                        for key in range(n, n + run_batch_size):
                            if key > operation_count:
                                break
                            tasks.add(executor.submit(self.db.cb_get, key))
                        results = MainLoop().task_wait(tasks)
                        data.extend(results)

                    print(f" == Retrieved {operation_count} records")
                    print(f" == Creating {output_file}")
                    df = pd.json_normalize(data)
                    df.to_csv(output_file, encoding='utf-8', index=False)
