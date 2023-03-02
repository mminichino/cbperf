##
##

import sys
import os
import inspect
import logging


class FatalError(Exception):

    def __init__(self, message):
        import traceback
        logging.debug(traceback.print_exc())
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        logging.debug("Error: {} in {} {} at line {}: {}".format(type(self).__name__, filename, function, line, message))
        logging.error(f"{message} [{filename}:{line}]")
        sys.exit(1)


class NonFatalError(Exception):

    def __init__(self, message):
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        self.message = "Error: {} in {} {} at line {}: {}".format(
            type(self).__name__, filename, function, line, message)
        logging.debug(f"Caught exception: {self.message}")
        super().__init__(self.message)


class ConfigFileError(FatalError):
    pass


class SchemaFileError(FatalError):
    pass


class ParameterError(FatalError):
    pass


class TestExecError(FatalError):
    pass


class RulesError(FatalError):
    pass


class TestConfigError(FatalError):
    pass


class InventoryConfigError(FatalError):
    pass


class TestRunError(FatalError):
    pass


class TestRunException(NonFatalError):
    pass


class ExportException(NonFatalError):
    pass


class ExportError(FatalError):
    pass


class DriverError(FatalError):
    pass
