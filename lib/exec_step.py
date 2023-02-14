##
##

import logging
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

    def __init__(self, db: CBConnect, key: str, document: dict, id_field: str = "record_id"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.key = key
        self.document = document
        self.id_field = id_field
        self.db = db
        self._result = None

    def execute(self):
        r = randomize()
        r.prepareTemplate(self.document)
        document = r.processTemplate()
        document[self.id_field] = self.key
        self._result = self.db.cb_upsert(self.key, document)

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
    def result(self):
        return self._result
