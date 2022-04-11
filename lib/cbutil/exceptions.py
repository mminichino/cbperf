##
##

import sys
import os
import inspect


class cbUtilError(Exception):

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


class HTTPException(cbUtilError):
    pass


class GeneralError(cbUtilError):
    pass


class NotAuthorized(cbUtilError):
    pass


class ForbiddenError(cbUtilError):
    pass


class NotFoundError(cbUtilError):
    pass


class ClusterInitError(cbUtilError):
    pass


class CbUtilEnvironmentError(cbUtilError):
    pass


class NodeUnreachable(cbUtilError):
    pass

