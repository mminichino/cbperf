#
#

import logging
import concurrent.futures
import lib.config as config
from lib.config import OperatingMode
from cbcmgr.cb_connect import CBConnect
from lib.exceptions import TestRunError
from lib.exec_step import DBRead, DBWrite, DBQuery
from lib.mptools import MPValue
from lib.schema import Bucket, Scope, Collection


class MainLoop(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def start(self):
        while True:
            for bucket in config.schema.buckets:
                for scope in bucket.scopes:
                    for collection in scope.collections:
                        self.logger.debug(f"exec: processing bucket {bucket.name}")
                        self.process(bucket, scope, collection)
            if not config.continuous:
                break

    def process(self, bucket: Bucket, scope: Scope, collection: Collection):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.batch_size)
        i = MPValue()

        try:
            db = CBConnect(config.host,
                           config.username,
                           config.password,
                           ssl=config.tls,
                           external=config.external_network).connect().create_bucket(bucket.name).create_scope(scope.name).create_collection(collection.name)
        except Exception as err:
            raise TestRunError(f"can not connect to Couchbase: {err}")

        if collection.override_count:
            operation_count = collection.override_count
        else:
            operation_count = config.count

        db_op = DBWrite(db, collection.schema, collection.idkey)
        logging.debug(f"process: collection {collection.name} count {operation_count}")

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
