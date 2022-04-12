##
##

import sys
import os
import inspect


class cbUtilError(Exception):

    def __init__(self, message):
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        print("Error: {} in {} {} at line {}: {}".format(type(self).__name__, filename, function, line, message))
        sys.exit(1)


class cbUtilException(Exception):

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class HTTPException(cbUtilError):
    pass


class GeneralError(cbUtilError):
    pass


class NotAuthorized(cbUtilError):
    pass


class ForbiddenError(cbUtilError):
    pass


class ClusterInitError(cbUtilError):
    pass


class CbUtilEnvironmentError(cbUtilError):
    pass


class NodeUnreachable(cbUtilError):
    pass


class NodeApiError(cbUtilError):
    pass


class AdminApiError(cbUtilError):
    pass


class ClusterConnectException(cbUtilException):
    pass


class BucketCreateException(cbUtilException):
    pass


class ScopeCreateException(cbUtilException):
    pass


class CollectionCreateException(cbUtilException):
    pass


class NotFoundError(cbUtilException):
    pass

