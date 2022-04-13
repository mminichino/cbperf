##
##

from .sessionmgr import cb_session
from .exceptions import *
from .retries import retry
from datetime import timedelta
from couchbase_core._libcouchbase import LOCKMODE_NONE
import couchbase
import acouchbase.cluster
from couchbase.cluster import Cluster, QueryOptions, ClusterTimeoutOptions, QueryIndexManager
from couchbase.management.buckets import CreateBucketSettings, BucketType
from couchbase.management.collections import CollectionSpec
from couchbase.auth import PasswordAuthenticator
import couchbase.subdocument as SD
from couchbase.exceptions import (CASMismatchException, CouchbaseException, CouchbaseTransientException,
                                  DocumentNotFoundException, DocumentExistsException, BucketDoesNotExistException,
                                  BucketAlreadyExistsException, DurabilitySyncWriteAmbiguousException,
                                  BucketNotFoundException, TimeoutException, QueryException, ScopeNotFoundException,
                                  ScopeAlreadyExistsException, CollectionAlreadyExistsException,
                                  CollectionNotFoundException, ProtocolException)
import logging
import asyncio
import json


class db_instance(object):

    def __init__(self):
        self.cluster_obj_s = None
        self.cluster_obj_a = None
        self.bm_obj = None
        self.qim_obj = None
        self.bucket_obj_s = None
        self.bucket_obj_a = None
        self.bucket_name = None
        self.scope_obj_s = None
        self.scope_obj_a = None
        self.scope_name = None
        self.collections_s = {}
        self.collections_a = {}

    def set_cluster(self, cluster_s, cluster_a):
        self.cluster_obj_s = cluster_s
        self.bm_obj = cluster_s.buckets()
        self.qim_obj = QueryIndexManager(cluster_s)
        self.cluster_obj_a = cluster_a

    def set_bucket(self, name, bucket_s, bucket_a):
        self.bucket_name = name
        self.bucket_obj_s = bucket_s
        self.bucket_obj_a = bucket_a

    def set_scope(self, name, scope_s, scope_a):
        self.scope_name = name
        self.scope_obj_s = scope_s
        self.scope_obj_a = scope_a

    def add_collection(self, name, collection_s, collection_a):
        self.collections_s[name] = collection_s
        self.collections_a[name] = collection_a

    @property
    def cluster_s(self):
        return self.cluster_obj_s

    @property
    def cluster_a(self):
        return self.cluster_obj_a

    @property
    def bm(self):
        return self.bm_obj

    @property
    def qim(self):
        return self.qim_obj

    @property
    def bucket_s(self):
        return self.bucket_obj_s

    @property
    def bucket_a(self):
        return self.bucket_obj_a

    @property
    def cm_s(self):
        cm_obj = self.bucket_obj_s.collections()
        return cm_obj

    @property
    def cm_a(self):
        cm_obj = self.bucket_obj_a.collections()
        return cm_obj

    @property
    def scope_s(self):
        return self.scope_obj_s

    @property
    def scope_a(self):
        return self.scope_obj_a

    def collection_s(self, name):
        if name in self.collections_s:
            return self.collections_s[name]
        else:
            raise CollectionNameNotFound("{} not configured".format(name))

    def collection_a(self, name):
        if name in self.collections_a:
            return self.collections_a[name]
        else:
            raise CollectionNameNotFound("{} not configured".format(name))

    def keyspace_s(self, name):
        if name in self.collections_s:
            keyspace = self.bucket_name + '.' + self.scope_name + '.' + name
            return keyspace
        else:
            raise CollectionNameNotFound("{} not configured".format(name))

    def keyspace_a(self, name):
        if name in self.collections_a:
            keyspace = self.bucket_name + '.' + self.scope_name + '.' + name
            return keyspace
        else:
            raise CollectionNameNotFound("{} not configured".format(name))


class cb_connect(cb_session):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = PasswordAuthenticator(self.username, self.password)
        self.timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=60), kv_timeout=timedelta(seconds=60))
        self.db = db_instance()

    @retry(allow_list=(ProtocolException, CouchbaseTransientException))
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
           allow_list=(CouchbaseTransientException, ProtocolException))
    async def bucket(self, name):
        bucket_s = self.db.cluster_s.bucket(name)
        bucket_a = self.db.cluster_a.bucket(name)
        await bucket_a.on_connect()
        self.db.set_bucket(name, bucket_s, bucket_a)

    def bucket_s(self, name):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.bucket(name))

    @retry(always_raise_list=(ScopeNotFoundException,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    async def scope(self, name):
        scope_s = self.db.bucket_s.scope(name)
        scope_a = self.db.bucket_a.scope(name)
        self.db.set_scope(name, scope_s, scope_a)

    def scope_s(self, name):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.scope(name))

    @retry(always_raise_list=(CollectionNotFoundException,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    async def collection(self, name):
        collection_s = self.db.scope_s.collection(name)
        collection_a = self.db.scope_a.collection(name)
        await collection_a.on_connect()
        self.db.add_collection(name, collection_s, collection_a)

    def collection_s(self, name):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.collection(name))

    @retry(always_raise_list=(BucketDoesNotExistException,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def is_bucket(self, name):
        try:
            return self.db.bm.get_bucket(name)
        except BucketDoesNotExistException:
            return None

    @retry(always_raise_list=(AttributeError,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def is_scope(self, scope):
        try:
            return next((s for s in self.db.cm_s.get_all_scopes() if s.name == scope), None)
        except AttributeError:
            return None

    @retry(always_raise_list=(AttributeError,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def is_collection(self, collection):
        try:
            return next((c for c in self.db.scope_s.collections if c.name == collection), None)
        except AttributeError:
            return None

    @retry(always_raise_list=(BucketAlreadyExistsException,),
           allow_list=(CouchbaseTransientException, ProtocolException))
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

    @retry(always_raise_list=(ScopeAlreadyExistsException,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def create_scope(self, name):
        try:
            self.db.cm_s.create_scope(name)
        except ScopeAlreadyExistsException:
            pass
        except Exception as err:
            raise ScopeCreateException("can not create scope {}: {}".format(name, err))

        self.scope_s(name)

    @retry(always_raise_list=(CollectionAlreadyExistsException,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def create_collection(self, name):
        try:
            collection_spec = CollectionSpec(name, scope_name=self.db.scope_name)
            collection_object = self.db.cm_s.create_collection(collection_spec)
        except CollectionAlreadyExistsException:
            pass
        except Exception as err:
            raise CollectionCreateException("can not create collection {}: {}".format(name, err))

        self.collection_s(name)

    @retry(always_raise_list=(DocumentNotFoundException, CollectionNameNotFound),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def cb_get_s(self, name, key):
        try:
            collection = self.db.collection_s(name)
            return collection.get(key)
        except DocumentNotFoundException:
            return None
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionGetError("can not get {} from {}: {}".format(key, name, err))

    @retry(always_raise_list=(DocumentNotFoundException, CollectionNameNotFound),
           allow_list=(CouchbaseTransientException, ProtocolException))
    async def cb_get_a(self, name, key):
        try:
            collection = self.db.collection_a(name)
            return await collection.get(key)
        except DocumentNotFoundException:
            return None
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionGetError("can not get {} from {}: {}".format(key, name, err))

    @retry(always_raise_list=(DocumentExistsException, CollectionNameNotFound),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def cb_upsert_s(self, name, key, document):
        try:
            collection = self.db.collection_s(name)
            result = collection.upsert(key, document)
            return result
        except DocumentExistsException:
            return None
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionUpsertError("can not upsert {} into {}: {}".format(key, name, err))

    @retry(always_raise_list=(DocumentExistsException, CollectionNameNotFound),
           allow_list=(CouchbaseTransientException, ProtocolException))
    async def cb_upsert_a(self, name, key, document):
        try:
            collection = self.db.collection_a(name)
            result = await collection.upsert(key, document)
            return result
        except DocumentExistsException:
            return None
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionUpsertError("can not upsert {} into {}: {}".format(key, name, err))

    @retry(always_raise_list=(CollectionNameNotFound,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def cb_subdoc_upsert_s(self, name, key, field, value):
        try:
            collection = self.db.collection_s(name)
            result = collection.mutate_in(key, [SD.upsert(field, value)])
            return result
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionSubdocUpsertError("can not update {} in {}: {}".format(key, field, err))

    @retry(always_raise_list=(CollectionNameNotFound,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    async def cb_subdoc_upsert_a(self, name, key, field, value):
        try:
            collection = self.db.collection_a(name)
            result = await collection.mutate_in(key, [SD.upsert(field, value)])
            return result
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionSubdocUpsertError("can not update {} in {}: {}".format(key, field, err))

    @retry(always_raise_list=(CollectionNameNotFound,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def cb_subdoc_get_s(self, name, key, field):
        try:
            collection = self.db.collection_s(name)
            result = collection.lookup_in(key, [SD.get(field)])
            return result
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionSubdocGetError("can not get {} from {}: {}".format(key, field, err))

    @retry(always_raise_list=(CollectionNameNotFound,),
           allow_list=(CouchbaseTransientException, ProtocolException))
    async def cb_subdoc_get_a(self, name, key, field):
        try:
            collection = self.db.collection_a(name)
            result = await collection.lookup_in(key, [SD.get(field)])
            return result
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionSubdocGetError("can not get {} from {}: {}".format(key, field, err))

    @retry(always_raise_list=(QueryException, TimeoutException, CollectionNameNotFound),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def cb_query_s(self, name, field, where=None, value=None):
        try:
            keyspace = self.db.keyspace_s(name)
            contents = []
            if not where:
                query = "SELECT " + field + " FROM " + keyspace + ";"
            else:
                query = "SELECT " + field + " FROM " + keyspace + " WHERE " + where + " = \"" + value + "\";"
            result = self.db.cluster_s.query(query, QueryOptions(metrics=False, adhoc=True, pipeline_batch=128,
                                                                 max_parallelism=4, pipeline_cap=1024, scan_cap=1024))
            for item in result:
                contents.append(item)
            return contents
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise QueryError("can not query {} from {}: {}".format(field, name, err))

    @retry(always_raise_list=(QueryException, TimeoutException, CollectionNameNotFound),
           allow_list=(CouchbaseTransientException, ProtocolException))
    async def cb_query_a(self, name, field, where=None, value=None):
        try:
            keyspace = self.db.keyspace_a(name)
            contents = []
            if not where:
                query = "SELECT " + field + " FROM " + keyspace + ";"
            else:
                query = "SELECT " + field + " FROM " + keyspace + " WHERE " + where + " = \"" + value + "\";"
            result = self.db.cluster_a.query(query, QueryOptions(metrics=False, adhoc=True, pipeline_batch=128,
                                                                 max_parallelism=4, pipeline_cap=1024, scan_cap=1024))
            async for item in result:
                contents.append(item)
            return contents
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise QueryError("can not query {} from {}: {}".format(field, name, err))

    @retry(always_raise_list=(DocumentNotFoundException, CollectionNameNotFound),
           allow_list=(CouchbaseTransientException, ProtocolException))
    def cb_remove_s(self, name, key):
        try:
            collection = self.db.collection_s(name)
            return collection.remove(key)
        except DocumentNotFoundException:
            return None
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionRemoveError("can not remove {} from {}: {}".format(key, name, err))

    @retry(always_raise_list=(DocumentNotFoundException, CollectionNameNotFound),
           allow_list=(CouchbaseTransientException, ProtocolException))
    async def cb_remove_a(self, name, key):
        try:
            collection = self.db.collection_a(name)
            return await collection.remove(key)
        except DocumentNotFoundException:
            return None
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise CollectionRemoveError("can not remove {} from {}: {}".format(key, name, err))
