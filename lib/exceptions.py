##
##

import sys
import os
import inspect
from lib.cbutil.cbconnect import cb_debug


class cbPerfFatal(Exception):

    def __init__(self, message):
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        print("Error: {} in {} {} at line {}: {}".format(type(self).__name__, filename, function, line, message))
        sys.exit(1)


class cbPerfException(Exception):

    def __init__(self, message):
        debug = cb_debug(self.__class__.__name__)
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        self.message = "Error: {} in {} {} at line {}: {}".format(
            type(self).__name__, filename, function, line, message)
        if debug.do_debug:
            logger = debug.logger
            logger.debug(self.message)
        debug.close()
        super().__init__(self.message)


class ConfigFileError(cbPerfFatal):
    pass


class SchemaFileError(cbPerfFatal):
    pass


class ParameterError(cbPerfFatal):
    pass


class TestExecError(cbPerfFatal):
    pass


class RulesError(cbPerfFatal):
    pass


class TestConfigError(cbPerfFatal):
    pass


class InventoryConfigError(cbPerfFatal):
    pass


class TestRunError(cbPerfFatal):
    pass


class TestRunException(cbPerfException):
    pass

