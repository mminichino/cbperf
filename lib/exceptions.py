##
##

import sys
import os
import inspect
import logging


class cbPerfFatal(Exception):

    def __init__(self, message):
        import traceback
        traceback.print_exc()
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        print("Error: {} in {} {} at line {}: {}".format(type(self).__name__, filename, function, line, message))
        sys.exit(1)


class cbPerfException(Exception):

    def __init__(self, message):
        logger = logging.getLogger(self.__class__.__name__)
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        self.message = "Error: {} in {} {} at line {}: {}".format(
            type(self).__name__, filename, function, line, message)
        logger.debug(f"Caught exception: {self.message}")
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
