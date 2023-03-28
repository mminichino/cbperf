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
        bucket_mem_quota = self.calc_mem_quota(size_total)
        self.logger.info(f"Creating bucket with quota {bucket_mem_quota}MiB")

        now = datetime.now()
        time_string = now.strftime("%D %I:%M:%S %p")
        self.logger.info(f"Import started at {time_string}")

        for table in self.schema.tables:
            start_time = time.perf_counter()
            collection = table.name
            self.logger.info(f"Processing table {table.name}")

            table_index_columns = self.plugin.get_table_indexes(table.name)

            try:
                self.logger.info(f"Creating collection {collection}")
                dbm = MainLoop().prep_bucket(bucket, scope, collection, bucket_mem_quota)
                if len(table_index_columns) > 0:
                    for column in table_index_columns:
                        self.logger.info(f"Creating index on {column}")
                        index_name = dbm.cb_create_index(fields=[column], replica=config.replicas)
                        if not index_name:
                            self.logger.info(f"Index already exists")
                        else:
                            self.logger.info(f"Created index {index_name}")
                db = CBConnect(config.host, config.username, config.password, ssl=config.tls).connect(bucket, scope, collection)
            except Exception as err:
                raise PluginImportError(f"can not connect to Couchbase: {err}")

            self.logger.info(f"Copying {table.rows:,} row(s) of table data to Couchbase (this step may take some time)")
            db_op = DBWrite(db)
            key_count = 0
            iteration_count = 0
            tasks.clear()
            for row in self.plugin.get_table(table):
                row_json = json.dumps(row, indent=2, default=self.json_serial)
                document = json.loads(row_json)
                key_count += 1
                tasks.add(executor.submit(db_op.execute, key_count, document))
                iteration_count += 1
                if iteration_count >= config.batch_size:
                    MainLoop().task_wait(tasks)
                    tasks.clear()
                    iteration_count = 0

            MainLoop().task_wait(tasks)
            tasks.clear()

            if table.rows != key_count:
                self.logger.warning(f"Actual rows {key_count} doesn't equal expected count {table.rows}")
            else:
                self.logger.info("All rows imported")

            end_time = time.perf_counter()
            run_time = time.strftime("%H hours %M minutes %S seconds", time.gmtime(end_time - start_time))
            self.logger.info(f"Table complete in {run_time}")

        now = datetime.now()
        time_string = now.strftime("%D %I:%M:%S %p")
        self.logger.info(f"Import complete at {time_string}")
