##
##

import logging
import time
from jinja2 import Template
from cbcmgr.cb_connect import CBConnect
from lib.cbutil.randomize import randomize


class DBRead(object):

    def __init__(self, db: CBConnect, key: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.key = key
        self.db = db
        self._result = None

    def execute(self):
        self._result = self.db.cb_get(self.key)

    @property
    def result(self):
        return self._result


class DBWrite(object):

    def __init__(self, db: CBConnect, document: dict, id_field: str = "record_id"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.document = document
        self.id_field = id_field
        self.db = db
        self._result = None

    def execute(self, key: str, template: bool = True):
        begin_time = time.time()
        if template:
            r = randomize()
            r.prepareTemplate(self.document)
            document = r.processTemplate()
        else:
            document = self.document
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
