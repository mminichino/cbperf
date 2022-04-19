##
##

from .sessionmgr import cb_session
from .exceptions import *
from .retries import retry
from .dbinstance import db_instance
from datetime import timedelta
from couchbase.options import LOCKMODE_NONE
from couchbase.diagnostics import PingState
import couchbase
import acouchbase.cluster
from couchbase.cluster import Cluster, QueryOptions, ClusterTimeoutOptions, QueryIndexManager
from couchbase.management.buckets import CreateBucketSettings, BucketType
from couchbase.management.collections import CollectionSpec
from couchbase.auth import PasswordAuthenticator
import couchbase.subdocument as SD
from couchbase.exceptions import (CASMismatchException, CouchbaseException, CouchbaseTransientException,
                                  DocumentNotFoundException, DocumentExistsException, BucketDoesNotExistException,
                                  BucketAlreadyExistsException, QueryErrorContext,
                                  BucketNotFoundException, TimeoutException, QueryException, ScopeNotFoundException,
                                  ScopeAlreadyExistsException, CollectionAlreadyExistsException,
                                  CollectionNotFoundException, ProtocolException)
import logging
import asyncio
import json


class cb_connect(cb_session):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = PasswordAuthenticator(self.username, self.password)
        self.timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=60), kv_timeout=timedelta(seconds=60))
        self.db = db_instance()

    def construct_key(self, key, collection):
        if type(key) == int or str(key).isdigit():
            if collection != "_default":
                return collection + ':' + str(key)
            else:
                return self.db.bucket_name + ':' + str(key)
        else:
            return key

    @retry(retry_count=10, allow_list=(ProtocolException, CouchbaseTransientException, TimeoutException))
    async def connect(self):
        cluster_s = couchbase.cluster.Cluster(self.cb_connect_string,
                                              authenticator=self.auth,
                                              lockmode=LOCKMODE_NONE,
                                              timeout_options=self.timeouts)
        cluster_a = acouchbase.cluster.Cluster(self.cb_connect_string,
                                               authenticator=self.auth,
                                               lockmode=LOCKMODE_NONE,
                                               timeout_options=self.timeouts)
        await cluster_a.on_connect()
        self.db.set_cluster(cluster_s, cluster_a)

    def connect_s(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.connect())

    @retry(always_raise_list=(BucketNotFoundException,),
           retry_count=10,
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException))
    async def bucket(self, name):
        bucket_s = self.db.cluster_s.bucket(name)
        bucket_a = self.db.cluster_a.bucket(name)
        await bucket_a.on_connect()
        self.db.set_bucket(name, bucket_s, bucket_a)

    @retry(always_raise_list=(BucketNotFoundException,),
           retry_count=10,
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException))
    def bucket_s(self, name):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.bucket(name))

    @retry(always_raise_list=(ScopeNotFoundException,),
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException))
    async def scope(self, name="_default"):
        scope_s = self.db.bucket_s.scope(name)
        scope_a = self.db.bucket_a.scope(name)
        self.db.set_scope(name, scope_s, scope_a)

    def scope_s(self, name="_default"):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.scope(name))

    @retry(always_raise_list=(CollectionNotFoundException,),
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException))
    async def collection(self, name="_default"):
        collection_s = self.db.scope_s.collection(name)
        collection_a = self.db.scope_a.collection(name)
        await collection_a.on_connect()
        self.db.add_collection(name, collection_s, collection_a)

    def collection_s(self, name="_default"):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.collection(name))

    @retry(always_raise_list=(BucketDoesNotExistException,),
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException))
    def is_bucket(self, name):
        try:
            return self.db.bm.get_bucket(name)
        except BucketDoesNotExistException:
            return None

    @retry(always_raise_list=(AttributeError,),
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException))
    def is_scope(self, scope):
        try:
            return next((s for s in self.db.cm_s.get_all_scopes() if s.name == scope), None)
        except AttributeError:
            return None

    @retry(always_raise_list=(AttributeError,),
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException))
    def is_collection(self, collection):
        try:
            return next((c for c in self.db.scope_s.collections if c.name == collection), None)
        except AttributeError:
            return None

    @retry(always_raise_list=(BucketAlreadyExistsException,),
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException))
    def create_bucket(self, name, quota=256):
        try:
            self.db.bm.create_bucket(CreateBucketSettings(name=name,
                                                          bucket_type=BucketType.COUCHBASE,
                                                          ram_quota_mb=quota))
        except BucketAlreadyExistsException:
            pass
        except Exception as err:
            raise BucketCreateException("can not create bucket {}: {}".format(name, err))

        self.bucket_s(name)

    @retry(always_raise_list=(BucketNotFoundException,),
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException))
    def drop_bucket(self, name):
        try:
            self.db.bm.drop_bucket(name)
        except BucketNotFoundException:
            pass
        except Exception as err:
            raise BucketDeleteException("can not drop bucket {}: {}".format(name, err))

        self.db.drop_bucket()

    @retry(always_raise_list=(ScopeAlreadyExistsException,),
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException))
    def create_scope(self, name):
        try:
            self.db.cm_s.create_scope(name)
        except ScopeAlreadyExistsException:
            pass
        except Exception as err:
            raise ScopeCreateException("can not create scope {}: {}".format(name, err))

        self.scope_s(name)

    @retry(always_raise_list=(CollectionAlreadyExistsException,),
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException))
    def create_collection(self, name):
        try:
            collection_spec = CollectionSpec(name, scope_name=self.db.scope_name)
            collection_object = self.db.cm_s.create_collection(collection_spec)
        except CollectionAlreadyExistsException:
            pass
        except Exception as err:
            raise CollectionCreateException("can not create collection {}: {}".format(name, err))

        self.collection_s(name)

    def collection_count(self, name="_default"):
        try:
            keyspace = self.db.keyspace_s(name)
            queryText = 'select count(*) as count from ' + keyspace + ';'
            result = self.cb_query_s(sql=queryText)
            return result[0]['count']
        except Exception as err:
            CollectionCountError("can not get item count for {}: {}".format(name, err))

    @retry(always_raise_list=(DocumentNotFoundException, CollectionNameNotFound),
           retry_count=10,
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException, CollectionGetError))
    def cb_get_s(self, key, name="_default"):
        try:
            document_id = self.construct_key(key, name)
            collection = self.db.collection_s(name)
            return collection.get(document_id)
        except DocumentNotFoundException:
            return None
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionGetError("can not get {} from {}: {}".format(key, name, err))

    @retry(always_raise_list=(DocumentNotFoundException, CollectionNameNotFound),
           retry_count=10,
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException, CollectionGetError))
    async def cb_get_a(self, key, name="_default"):
        try:
            document_id = self.construct_key(key, name)
            collection = self.db.collection_a(name)
            return await collection.get(document_id)
        except DocumentNotFoundException:
            return None
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionGetError("can not get {} from {}: {}".format(key, name, err))

    @retry(always_raise_list=(DocumentExistsException, CollectionNameNotFound),
           retry_count=10,
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException, CollectionUpsertError))
    def cb_upsert_s(self, key, document, name="_default"):
        try:
            document_id = self.construct_key(key, name)
            collection = self.db.collection_s(name)
            result = collection.upsert(document_id, document)
            return result
        except DocumentExistsException:
            return None
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionUpsertError("can not upsert {} into {}: {}".format(key, name, err))

    @retry(always_raise_list=(DocumentExistsException, CollectionNameNotFound),
           retry_count=10,
           allow_list=(CouchbaseTransientException, ProtocolException, TimeoutException, CollectionUpsertError))
    async def cb_upsert_a(self, key, document, name="_default"):
        try:
            document_id = self.construct_key(key, name)
            collection = self.db.collection_a(name)
            result = await collection.upsert(document_id, document)
            return result
        except DocumentExistsException:
            return None
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionUpsertError("can not upsert {} into {}: {}".format(key, name, err))

    @retry(always_raise_list=(CollectionNameNotFound,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def cb_subdoc_upsert_s(self, key, field, value, name="_default"):
        try:
            document_id = self.construct_key(key, name)
            collection = self.db.collection_s(name)
            result = collection.mutate_in(document_id, [SD.upsert(field, value)])
            return result
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionSubdocUpsertError("can not update {} in {}: {}".format(key, field, err))

    @retry(always_raise_list=(CollectionNameNotFound,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    async def cb_subdoc_upsert_a(self, key, field, value, name="_default"):
        try:
            document_id = self.construct_key(key, name)
            collection = self.db.collection_a(name)
            result = await collection.mutate_in(document_id, [SD.upsert(field, value)])
            return result
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionSubdocUpsertError("can not update {} in {}: {}".format(key, field, err))

    @retry(always_raise_list=(CollectionNameNotFound,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def cb_subdoc_get_s(self, key, field, name="_default"):
        try:
            document_id = self.construct_key(key, name)
            collection = self.db.collection_s(name)
            result = collection.lookup_in(document_id, [SD.get(field)])
            return result
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionSubdocGetError("can not get {} from {}: {}".format(key, field, err))

    @retry(always_raise_list=(CollectionNameNotFound,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    async def cb_subdoc_get_a(self, key, field, name="_default"):
        try:
            document_id = self.construct_key(key, name)
            collection = self.db.collection_a(name)
            result = await collection.lookup_in(document_id, [SD.get(field)])
            return result
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionSubdocGetError("can not get {} from {}: {}".format(key, field, err))

    def query_sql_constructor(self, field=None, name="_default", where=None, value=None, sql=None):
        if not where and not sql and field:
            keyspace = self.db.keyspace_a(name)
            query = "SELECT " + field + " FROM " + keyspace + ";"
        elif not sql and field:
            keyspace = self.db.keyspace_a(name)
            query = "SELECT " + field + " FROM " + keyspace + " WHERE " + where + " = \"" + str(value) + "\";"
        elif sql:
            query = sql
        else:
            raise QueryArgumentsError("query: either field or sql argument is required")

        return query

    @retry(always_raise_list=(QueryException, TimeoutException, CollectionNameNotFound, QueryArgumentsError),
           allow_list=(CouchbaseTransientException, ProtocolException, TransientError))
    def cb_query_s(self, field=None, name="_default", where=None, value=None, sql=None):
        query = ""
        try:
            contents = []
            query = self.query_sql_constructor(field, name, where, value, sql)
            result = self.db.cluster_s.query(query, QueryOptions(metrics=False, adhoc=True, pipeline_batch=128,
                                                                 max_parallelism=4, pipeline_cap=1024, scan_cap=1024))
            for item in result:
                contents.append(item)
            return contents
        except CollectionNameNotFound:
            raise
        except CouchbaseException as err:
            error_class = decode_error_code(err.context.first_error_code)
            raise error_class(err.context.first_error_message)
        except Exception as err:
            raise QueryError("{}: can not query {} from {}: {}".format(query, field, name, err))

    @retry(always_raise_list=(QueryException, TimeoutException, CollectionNameNotFound, QueryArgumentsError),
           allow_list=(CouchbaseTransientException, ProtocolException, TransientError))
    async def cb_query_a(self, field=None, name="_default", where=None, value=None, sql=None):
        query = ""
        try:
            contents = []
            query = self.query_sql_constructor(field, name, where, value, sql)
            result = self.db.cluster_a.query(query, QueryOptions(metrics=False, adhoc=True, pipeline_batch=128,
                                                                 max_parallelism=4, pipeline_cap=1024, scan_cap=1024))
            async for item in result:
                contents.append(item)
            return contents
        except CollectionNameNotFound:
            raise
        except CouchbaseException as err:
            error_class = decode_error_code(err.context.first_error_code)
            raise error_class(err.context.first_error_message)
        except Exception as err:
            raise QueryError("{}: can not query {} from {}: {}".format(query, field, name, err))

    @retry(always_raise_list=(DocumentNotFoundException, CollectionNameNotFound),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def cb_remove_s(self, key, name="_default"):
        try:
            document_id = self.construct_key(key, name)
            collection = self.db.collection_s(name)
            return collection.remove(document_id)
        except DocumentNotFoundException:
            return None
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionRemoveError("can not remove {} from {}: {}".format(key, name, err))

    @retry(always_raise_list=(DocumentNotFoundException, CollectionNameNotFound),
           allow_list=(CouchbaseTransientException, ProtocolException))
    async def cb_remove_a(self, key, name="_default"):
        try:
            document_id = self.construct_key(key, name)
            collection = self.db.collection_a(name)
            return await collection.remove(document_id)
        except DocumentNotFoundException:
            return None
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionRemoveError("can not remove {} from {}: {}".format(key, name, err))
