##
##

import sys
import os
import inspect


class httpError(Exception):

    def __init__(self, message):
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        print("Error: {} in {} {} at line {}: {}".format(type(self).__name__, filename, function, line, message))
        sys.exit(1)


class httpException(Exception):

    def __init__(self, message):
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        self.message = "Error: {} in {} {} at line {}: {}".format(
            type(self).__name__, filename, function, line, message)
        super().__init__(self.message)


class MissingAuthKey(httpError):
    pass


class MissingSecretKey(httpError):
    pass


class MissingClusterName(httpError):
    pass


class HTTPException(httpException):
    pass


class GeneralError(httpError):
    pass


class NotAuthorized(httpError):
    pass


class HTTPForbidden(httpException):
    pass


class RequestValidationError(httpException):
    pass


class InternalServerError(httpException):
    pass


class ClusterNotFound(httpError):
    pass


class ConnectException(httpException):
    pass


class HTTPNotImplemented(httpException):
    pass


class PaginationDataNotFound(httpException):
    pass


class SyncGatewayOperationException(httpException):
    pass

