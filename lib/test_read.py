##
##

import logging


class TestStep(object):

    def __int__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self):
        pass

    def result(self):
        pass
