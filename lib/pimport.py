##
##

import logging
import json
import time
import concurrent.futures
from lib.plugins.relational import Schema, Table
from datetime import date, datetime
from cbcmgr.cb_connect import CBConnect
import lib.config as config
from lib.exceptions import PluginImportError
from lib.main import MainLoop
from lib.exec_step import DBWrite


class PluginImport(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        module = __import__(f"lib.plugins.{config.plugin_name}", fromlist=['*'])
        self.plugin = module.DBDriver(config.plugin_vars)
        self.schema = None

    def get_schema(self):
        self.schema: Schema = self.plugin.get_schema()

    def get_table(self, table: Table):
        pass

    @staticmethod
    def json_serial(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError("Type %s not serializable" % type(obj))

    @staticmethod
    def calc_mem_quota(n: int):
        return 1024 * round(n*4/1024)

    def import_tables(self):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.batch_size)
        bucket = config.bucket_name
        scope = config.scope_name
        tasks = set()

        self.logger.info(f"Retrieving schema information")
        self.get_schema()

        size_total = sum(list(map(lambda t: t.size, [table for table in self.schema.tables])))
        rows_total = sum(list(map(lambda t: t.rows, [table for table in self.schema.tables])))
        self.logger.info(f"Importing {rows_total:,} rows")

        for table in self.schema.tables:
            collection = table.name
            self.logger.info(f"Processing table {table.name}")

            try:
                MainLoop().prep_bucket(bucket, scope, collection, self.calc_mem_quota(size_total))
                db = CBConnect(config.host, config.username, config.password, ssl=config.tls).connect(bucket, scope, collection)
            except Exception as err:
                raise PluginImportError(f"can not connect to Couchbase: {err}")

            db_op = DBWrite(db)
            key_count = 0
            factor = 0.01
            for row in self.plugin.get_table(table):
                row_json = json.dumps(row, indent=2, default=self.json_serial)
                document = json.loads(row_json)
                key_count += 1
                tasks.add(executor.submit(db_op.execute, key_count, document))

                retry_number = 0
                while executor._work_queue.qsize() > config.batch_size:
                    wait = factor
                    wait *= (2 ** (retry_number + 1))
                    time.sleep(wait)

            MainLoop().task_wait(tasks)
