##
##

from .sessionmgr import cb_session
from .exceptions import *
from .dbinstance import db_instance
from .cbconnect import cb_connect
from .retries import retry
import couchbase
from couchbase.auth import PasswordAuthenticator
from couchbase_core._libcouchbase import LOCKMODE_NONE
from couchbase.cluster import Cluster, QueryOptions, ClusterTimeoutOptions, QueryIndexManager
from couchbase.exceptions import (CASMismatchException, CouchbaseException, CouchbaseTransientException,
                                  DocumentNotFoundException, DocumentExistsException, BucketDoesNotExistException,
                                  BucketAlreadyExistsException, DurabilitySyncWriteAmbiguousException,
                                  BucketNotFoundException, TimeoutException, QueryException, ScopeNotFoundException,
                                  ScopeAlreadyExistsException, CollectionAlreadyExistsException,
                                  CollectionNotFoundException, ProtocolException)
import logging
import json
from datetime import timedelta


class cb_index(cb_connect):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.connect_s()

    def connect_bucket(self, name):
        try:
            self.bucket_s(name)
        except Exception as err:
            raise IndexBucketError("can not connect to bucket {}: {}".format(name, err))

    def connect_scope(self, name="_default"):
        try:
            self.scope_s(name)
        except Exception as err:
            raise IndexScopeError("can not connect to scope {}: {}".format(name, err))

    def connect_collection(self, name="_default"):
        try:
            self.collection_s(name)
        except Exception as err:
            raise IndexCollectionError("can not connect to collection {}: {}".format(name, err))

    def is_index(self, field=None, name="_default"):
        if name != "_default" and field:
            index = name + '_' + field + '_ix'
        elif field:
            index = field + '_ix'
        else:
            index = None

        try:
            keyspace = self.db.keyspace_s(name)
            indexList = self.db.qim.get_all_indexes(self.db.bucket_name)
            for i in range(len(indexList)):
                if not index:
                    if indexList[i].keyspace == keyspace and indexList[i].name == '#primary':
                        return True
                if indexList[i].name == index:
                    return True
        except Exception as err:
            raise IndexStatError("Could not get index status: {}".format(err))

        return False

    @retry(always_raise_list=(QueryException, TimeoutException, CollectionNameNotFound),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def create_index(self, name="_default", field=None, replica=1):
        if name != "_default" and field:
            index = name + '_' + field + '_ix'
        elif field:
            index = field + '_ix'
        else:
            index = None

        try:
            keyspace = self.db.keyspace_s(name)
            if field and index:
                queryText = 'CREATE INDEX ' + index + ' ON ' + keyspace + '(' + field + ') WITH {"num_replica": ' \
                            + str(replica) + '};'
            else:
                queryText = 'CREATE PRIMARY INDEX ON ' + keyspace + ' WITH {"num_replica": ' + str(replica) + '};'
            result = self.cb_query_s(sql=queryText)
            return result
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise IndexQueryError("can not create index on {}: {}".format(name, err))

    @retry(always_raise_list=(QueryException, TimeoutException, CollectionNameNotFound),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def drop_index(self, name="_default", field=None):
        if name != "_default" and field:
            index = name + '_' + field + '_ix'
        elif field:
            index = field + '_ix'
        else:
            index = None

        try:
            keyspace = self.db.keyspace_s(name)
            if field and index:
                queryText = 'DROP INDEX ' + index + ' ON ' + keyspace + ' USING GSI;'
            else:
                queryText = 'DROP PRIMARY INDEX ON' + keyspace + ' USING GSI;'
            result = self.cb_query_s(sql=queryText)
            return result
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise IndexQueryError("can not drop index on {}: {}".format(name, err))

    @retry(factor=0.5, allow_list=(IndexNotReady,))
    def index_wait(self, name="_default", field=None):
        if name != "_default" and field:
            index = name + '_' + field + '_ix'
        elif field:
            index = field + '_ix'
        else:
            index = '#primary'

        if name == "_default":
            lookup = self.db.bucket_name
        else:
            lookup = name

        index_stats = self.index_stats()
        record_count = self.collection_count(name)

        for key in index_stats:
            if key == lookup:
                for item in index_stats[key]:
                    if item == index:
                        pending = index_stats[key][item]['num_docs_pending']
                        queued = index_stats[key][item]['num_docs_queued']
                        count = index_stats[key][item]['items_count']
                        if (pending != 0 and queued != 0) or count < record_count:
                            raise IndexNotReady("{} not ready, count {} pending {} queued {}".format(
                                index, count, pending, queued))

    def index_stats(self, name=None):
        if not name:
            bucket = self.db.bucket_name
        else:
            bucket = name

        index_data = {}
        endpoint = '/api/v1/stats/' + bucket
        for response_json in list(self.node_api_get(endpoint)):

            for key in response_json:
                index_name = key.split(':')[-1]
                index_object = key.split(':')[-2]
                if index_object not in index_data:
                    index_data[index_object] = {}
                if index_name not in index_data[index_object]:
                    index_data[index_object][index_name] = response_json[key]

        return index_data
