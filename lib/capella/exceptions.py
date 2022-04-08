##
##

import sys
import os
import inspect


class capellaError(Exception):

    def __init__(self, message):
        try:
            except_type, except_obj, except_trace = sys.exc_info()
            filename = os.path.split(except_trace.tb_frame.f_code.co_filename)[1]
            line = except_trace.tb_lineno
            print("Error: {} in {} at {}: {}".format(except_type, filename, line, message))
        except Exception:
            frame = inspect.currentframe().f_back
            (filename, line, function, lines, index) = inspect.getframeinfo(frame)
            filename = os.path.basename(filename)
            print("Error: {} in {} {} at line {}: {}".format(type(self).__name__, filename, function, line, message))
        sys.exit(1)


class MissingAuthKey(capellaError):
    pass


class MissingSecretKey(capellaError):
    pass


class HTTPException(capellaError):
    pass


class GeneralError(capellaError):
    pass


class NotAuthorized(capellaError):
    pass


class RequestValidationError(capellaError):
    pass


class InternalServerError(capellaError):
    pass


class ClusterNotFound(capellaError):
    pass

