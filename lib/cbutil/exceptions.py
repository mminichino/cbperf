##
##

import sys
import os
import re
import inspect
import logging


def decode_error_code(code, message):
    if code == 4300:
        return IndexExistsError
    elif code == 5000:
        if re.match(".*Index .* already exists.*", message):
            return IndexExistsError
        else:
            return TransientError
    elif code == 12003:
        return TransientError
    else:
        return CouchbaseError


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
        self.message = "Error: {} in {} {} at line {}: {}".format(type(self).__name__, filename, function, line, message)
        logging.debug(f"Caught exception: {self.message}")
        super().__init__(self.message)


class HTTPExceptionError(FatalError):
    pass


class GeneralError(FatalError):
    pass


class NotAuthorized(FatalError):
    pass


class ForbiddenError(FatalError):
    pass


class ClusterInitError(NonFatalError):
    pass


class ClusterCloseError(NonFatalError):
    pass


class CbUtilEnvironmentError(FatalError):
    pass


class NodeUnreachable(NonFatalError):
    pass


class NodeConnectionTimeout(NonFatalError):
    pass


class NodeConnectionError(NonFatalError):
    pass


class NodeConnectionFailed(NonFatalError):
    pass


class DNSLookupTimeout(NonFatalError):
    pass


class NodeApiError(FatalError):
    pass


class AdminApiError(FatalError):
    pass


class CollectionGetError(NonFatalError):
    pass


class CollectionUpsertError(NonFatalError):
    pass


class CollectionSubdocUpsertError(NonFatalError):
    pass


class CollectionSubdocGetError(NonFatalError):
    pass


class CollectionRemoveError(NonFatalError):
    pass


class CollectionCountError(NonFatalError):
    pass


class CollectionWaitException(NonFatalError):
    pass


class CollectionCountException(NonFatalError):
    pass


class ScopeWaitException(NonFatalError):
    pass


class BucketWaitException(NonFatalError):
    pass


class QueryError(NonFatalError):
    pass


class QueryEmptyException(NonFatalError):
    pass


class QueryArgumentsError(FatalError):
    pass


class IndexStatError(FatalError):
    pass


class IndexConnectError(FatalError):
    pass


class IndexBucketError(FatalError):
    pass


class IndexScopeError(FatalError):
    pass


class IndexQueryError(FatalError):
    pass


class IndexCollectionError(FatalError):
    pass


class IndexInternalError(FatalError):
    pass


class ClusterConnectException(NonFatalError):
    pass


class BucketCreateException(NonFatalError):
    pass


class BucketDeleteException(NonFatalError):
    pass


class ScopeCreateException(NonFatalError):
    pass


class IsCollectionException(NonFatalError):
    pass


class CollectionCreateException(NonFatalError):
    pass


class NotFoundError(NonFatalError):
    pass


class CollectionNameNotFound(NonFatalError):
    pass


class IndexNotReady(NonFatalError):
    pass


class ClusterHealthCheckError(NonFatalError):
    pass


class ClusterKVServiceError(NonFatalError):
    pass


class ClusterQueryServiceError(NonFatalError):
    pass


class ClusterViewServiceError(NonFatalError):
    pass


class CouchbaseError(NonFatalError):
    pass


class IndexExistsError(NonFatalError):
    pass


class IndexNotFoundError(NonFatalError):
    pass


class TransientError(NonFatalError):
    pass


class TestPauseError(NonFatalError):
    pass


class BucketStatsError(NonFatalError):
    pass


class BucketNotFound(NonFatalError):
    pass


class CollectionNotDefined(NonFatalError):
    pass
