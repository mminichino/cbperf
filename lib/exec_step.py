##
##

import logging
import time
from jinja2 import Template
from cbcmgr.cb_connect import CBConnect


class DBRead(object):

    def __init__(self, db: CBConnect, add_key: bool = False, key_field: str = 'doc_id'):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db = db
        self._add_key = add_key
        self._key_field = key_field
        self._result = None

    def execute(self, key: str):
        self._result = self.db.cb_get(key)
        self.add_key(key)

    @property
    def result(self):
        return self._result

    def add_key(self, key: str):
        if self._result and self._add_key:
            self._result[self._key_field] = key

    def fetch(self, key: str):
        self.execute(key)
        return self.result


class DBWrite(object):

    def __init__(self, db: CBConnect, id_field: str = "record_id"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.id_field = id_field
        self.db = db
        self._result = None

    def execute(self, key: str, document: dict):
        begin_time = time.time()
        document[self.id_field] = key
        self._result = self.db.cb_upsert(key, document)
        end_time = time.time()
        total_time = end_time - begin_time
        self.logger.debug(f"write complete in {total_time:.6f}")

    @property
    def result(self):
        return self._result


class DBQuery(object):

    def __init__(self, db: CBConnect, query: str, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.query = query
        self.query_params = kwargs
        self.db = db
        self._result = None

    def execute(self):
        if self.query_params:
            t = Template(self.query)
            self.query = t.render(**self.query_params)
        self._result = self.db.cb_query(sql=self.query)

    @property
    def keyspace(self):
        return self.db.keyspace

    @property
    def result(self):
        return self._result
