##
##

import logging
from cbcmgr.cb_connect import CBConnect


class TestRead(object):

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
