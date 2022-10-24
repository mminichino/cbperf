##
##

import logging


class TestDriver(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def create(self, schema: dict):
        pass

    def write_kv(self, parameters: dict):
        pass

    def write_sql(self, parameters: dict):
        pass

    def read_kv(self, parameters: dict):
        pass

    def read_sql(self, parameters: dict):
        pass

    def destroy(self, schema: dict):
        pass
