##
##

import sys
import os
import inspect


def decode_error_code(code):
    if code == 219:
        return IndexExistsError
    else:
        return CouchbaseError


class cbUtilError(Exception):

    def __init__(self, message):
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        print("Error: {} in {} {} at line {}: {}".format(type(self).__name__, filename, function, line, message))
        sys.exit(1)


class cbUtilException(Exception):

    def __init__(self, message):
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        self.message = "Error: {} in {} {} at line {}: {}".format(
            type(self).__name__, filename, function, line, message)
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


class CollectionGetError(cbUtilError):
    pass


class CollectionUpsertError(cbUtilError):
    pass


class CollectionSubdocUpsertError(cbUtilError):
    pass


class CollectionSubdocGetError(cbUtilError):
    pass


class CollectionRemoveError(cbUtilError):
    pass


class CollectionCountError(cbUtilError):
    pass


class QueryError(cbUtilError):
    pass


class QueryArgumentsError(cbUtilError):
    pass


class IndexStatError(cbUtilError):
    pass


class IndexBucketError(cbUtilError):
    pass


class IndexScopeError(cbUtilError):
    pass


class IndexQueryError(cbUtilError):
    pass


class IndexCollectionError(cbUtilError):
    pass


class ClusterConnectException(cbUtilException):
    pass


class BucketCreateException(cbUtilException):
    pass


class BucketDeleteException(cbUtilException):
    pass


class ScopeCreateException(cbUtilException):
    pass


class CollectionCreateException(cbUtilException):
    pass


class NotFoundError(cbUtilException):
    pass


class CollectionNameNotFound(cbUtilException):
    pass


class IndexNotReady(cbUtilException):
    pass


class ClusterHealthCheckError(cbUtilException):
    pass


class CouchbaseError(cbUtilException):
    pass


class IndexExistsError(cbUtilException):
    pass

