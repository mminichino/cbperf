##
##

from .sessionmgr import cb_session
from .exceptions import *
from .retries import retry, retry_a
from .dbinstance import db_instance
from .cbdebug import cb_debug
from datetime import timedelta
from couchbase.options import LOCKMODE_NONE
import couchbase
import acouchbase.cluster
from couchbase.cluster import Cluster, QueryOptions, ClusterTimeoutOptions
from couchbase.management.buckets import CreateBucketSettings, BucketType
from couchbase.management.collections import CollectionSpec
from couchbase.auth import PasswordAuthenticator
import couchbase.subdocument as SD
from couchbase.exceptions import (CouchbaseException,
                                  DocumentNotFoundException, DocumentExistsException, BucketDoesNotExistException,
                                  BucketAlreadyExistsException, HTTPException,
                                  BucketNotFoundException, QueryException, ScopeNotFoundException,
                                  ScopeAlreadyExistsException, CollectionAlreadyExistsException,
                                  CollectionNotFoundException)
import logging
import asyncio


class cb_connect(cb_session):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = PasswordAuthenticator(self.username, self.password)
        self.timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=240), kv_timeout=timedelta(seconds=240))
        self.db = db_instance()
        self.debugger = cb_debug(self.__class__.__name__)
        self.debug = self.debugger.do_debug
        self.logger = self.debugger.logger

    def construct_key(self, key, collection):
        if type(key) == int or str(key).isdigit():
            if collection != "_default":
                return collection + ':' + str(key)
            else:
                return self.db.bucket_name + ':' + str(key)
        else:
            return key

    def unhandled_exception(self, loop, context):
        err = context.get("exception", context['message'])
        if isinstance(err, Exception):
            self.logger.error(f"unhandled exception: type: {err.__class__.__name__} msg: {err} cause: {err.__cause__}")
        else:
            self.logger.error(f"unhandled error: {err}")

    @retry_a(factor=0.5, retry_count=10)
    async def connect_a(self):
        tasks = []
        try:
            cluster_a = acouchbase.cluster.Cluster(self.cb_connect_string, authenticator=self.auth, lockmode=LOCKMODE_NONE, timeout_options=self.timeouts)
            tasks.append(asyncio.create_task(cluster_a.on_connect()))
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    raise result
            self.db.set_cluster_a(cluster_a)
            return results
        except SystemError as err:
            if isinstance(err.__cause__, HTTPException):
                raise ClusterConnectException("cluster connect HTTP error: {}".format(err.__cause__))
            else:
                raise ClusterConnectException("cluster connect network error: {}".format(err))
        except HTTPException as err:
            raise ClusterConnectException("cluster connect HTTP error: {}".format(err))
        except Exception as err:
            raise ClusterConnectException("cluster connect general error: {}".format(err))

    @retry(factor=0.5, retry_count=10)
    def connect_s(self):
        try:
            cluster_s = couchbase.cluster.Cluster(self.cb_connect_string, authenticator=self.auth, lockmode=LOCKMODE_NONE, timeout_options=self.timeouts)
            self.db.set_cluster_s(cluster_s)
            return True
        except SystemError as err:
            if isinstance(err.__cause__, HTTPException):
                raise ClusterConnectException("cluster connect HTTP error: {}".format(err.__cause__))
            else:
                raise ClusterConnectException("cluster connect network error: {}".format(err))
        except HTTPException as err:
            raise ClusterConnectException("cluster connect HTTP error: {}".format(err))
        except Exception as err:
            raise ClusterConnectException("cluster connect general error: {}".format(err))

    async def disconnect_a(self):
        self.db.cluster_a.disconnect()

    def disconnect_s(self):
        self.db.cluster_s.disconnect()

    @retry_a(always_raise_list=(BucketNotFoundException,), retry_count=10, factor=0.5)
    async def bucket_a(self, name):
        bucket_a = self.db.cluster_a.bucket(name)
        await bucket_a.on_connect()
        self.db.set_bucket_a(name, bucket_a)

    @retry(always_raise_list=(BucketNotFoundException,), retry_count=10, factor=0.5)
    def bucket_s(self, name):
        bucket_s = self.db.cluster_s.bucket(name)
        self.db.set_bucket_s(name, bucket_s)

    @retry_a(always_raise_list=(ScopeNotFoundException,), retry_count=10)
    async def scope_a(self, name="_default"):
        scope_a = self.db.bucket_a.scope(name)
        self.db.set_scope_a(name, scope_a)

    @retry(always_raise_list=(ScopeNotFoundException,), retry_count=10)
    def scope_s(self, name="_default"):
        scope_s = self.db.bucket_s.scope(name)
        self.db.set_scope_s(name, scope_s)

    @retry_a(always_raise_list=(CollectionNotFoundException,), retry_count=10)
    async def collection_a(self, name="_default"):
        collection_a = self.db.scope_a.collection(name)
        await collection_a.on_connect()
        self.db.add_collection_a(name, collection_a)

    @retry(always_raise_list=(CollectionNotFoundException,), retry_count=10)
    def collection_s(self, name="_default"):
        collection_s = self.db.scope_s.collection(name)
        self.db.add_collection_s(name, collection_s)

    @retry_a(retry_count=10)
    async def quick_connect_a(self, bucket, scope, collection):
        try:
            await self.connect_a()
            await self.bucket_a(bucket)
            await self.scope_a(scope)
            await self.collection_a(collection)
            return True
        except Exception as err:
            raise ClusterConnectException(f"quick connect error: {err}")

    @retry(retry_count=10)
    def quick_connect_s(self, bucket, scope, collection):
        try:
            self.connect_s()
            self.bucket_s(bucket)
            self.scope_s(scope)
            self.collection_s(collection)
            return True
        except Exception as err:
            raise ClusterConnectException(f"quick connect error: {err}")

    @retry(always_raise_list=(BucketDoesNotExistException,), retry_count=10)
    def is_bucket(self, name):
        try:
            return self.db.bm.get_bucket(name)
        except BucketDoesNotExistException:
            return None

    @retry(always_raise_list=(AttributeError,), retry_count=10)
    def is_scope(self, scope):
        try:
            return next((s for s in self.db.cm_s.get_all_scopes() if s.name == scope), None)
        except AttributeError:
            return None

    @retry(always_raise_list=(AttributeError,), retry_count=10)
    def is_collection(self, collection):
        try:
            scope_spec = next((s for s in self.db.cm_s.get_all_scopes() if s.name == self.db.scope_name), None)
            if not scope_spec:
                raise ScopeNotFoundException(f"is_collection: no scope configured")
            return next((c for c in scope_spec.collections if c.name == collection), None)
        except AttributeError:
            return None

    @retry(retry_count=10)
    def collection_wait(self, collection):
        if not self.is_collection(collection):
            raise CollectionWaitException(f"waiting: collection {collection} does not exist")

    @retry(retry_count=10)
    def scope_wait(self, scope):
        if not self.is_scope(scope):
            raise ScopeWaitException(f"waiting: scope {scope} does not exist")

    @retry(retry_count=10)
    def bucket_wait(self, bucket):
        if not self.is_bucket(bucket):
            raise BucketWaitException(f"waiting: bucket {bucket} does not exist")

    @retry(always_raise_list=(BucketAlreadyExistsException,), retry_count=10)
    def create_bucket(self, name, quota=256):
        try:
            if self.capella_target:
                try:
                    self.is_bucket(name)
                except BucketDoesNotExistException:
                    self.capella_session.capella_create_bucket(name, quota)
            else:
                self.db.bm.create_bucket(CreateBucketSettings(name=name,
                                                              bucket_type=BucketType.COUCHBASE,
                                                              ram_quota_mb=quota))
        except BucketAlreadyExistsException:
            pass
        except Exception as err:
            raise BucketCreateException("can not create bucket {}: {}".format(name, err))

        self.bucket_s(name)

    @retry(always_raise_list=(BucketNotFoundException,), retry_count=10)
    def drop_bucket(self, name):
        try:
            if self.capella_target:
                try:
                    self.is_bucket(name)
                    self.capella_session.capella_delete_bucket(name)
                except BucketDoesNotExistException:
                    return True
            else:
                self.db.bm.drop_bucket(name)
        except BucketNotFoundException:
            pass
        except Exception as err:
            raise BucketDeleteException("can not drop bucket {}: {}".format(name, err))

        self.db.drop_bucket()

    @retry(always_raise_list=(ScopeAlreadyExistsException,), retry_count=10)
    def create_scope(self, name):
        try:
            self.db.cm_s.create_scope(name)
        except ScopeAlreadyExistsException:
            pass
        except Exception as err:
            raise ScopeCreateException("can not create scope {}: {}".format(name, err))

        self.scope_s(name)

    @retry(always_raise_list=(CollectionAlreadyExistsException,), retry_count=10)
    def create_collection(self, name):
        try:
            collection_spec = CollectionSpec(name, scope_name=self.db.scope_name)
            collection_object = self.db.cm_s.create_collection(collection_spec)
        except CollectionAlreadyExistsException:
            pass
        except Exception as err:
            raise CollectionCreateException("can not create collection {}: {}".format(name, err))

        self.collection_s(name)

    @retry(always_raise_list=(CollectionNotFoundException,), retry_count=10)
    def drop_collection(self, name):
        try:
            collection_spec = CollectionSpec(name, scope_name=self.db.scope_name)
            collection_object = self.db.cm_s.drop_collection(collection_spec)
        except CollectionNotFoundException:
            pass
        except Exception as err:
            raise CollectionRemoveError("can not drop collection {}: {}".format(name, err))

        self.db.drop_collection(name)

    @retry(retry_count=10)
    def collection_count_s(self, name="_default"):
        try:
            keyspace = self.db.keyspace_s(name)
            queryText = 'select count(*) as count from ' + keyspace + ';'
            result = self.cb_query_s(sql=queryText)
            return result[0]['count']
        except Exception as err:
            CollectionCountError("can not get item count for {}: {}".format(name, err))

    @retry_a(retry_count=10)
    async def collection_count_a(self, name="_default"):
        try:
            keyspace = self.db.keyspace_a(name)
            queryText = 'select count(*) as count from ' + keyspace + ';'
            result = await self.cb_query_a(sql=queryText)
            return result[0]['count']
        except Exception as err:
            CollectionCountError("can not get item count for {}: {}".format(name, err))

    @retry(always_raise_list=(DocumentNotFoundException, CollectionNameNotFound), retry_count=10)
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

    @retry_a(always_raise_list=(DocumentNotFoundException, CollectionNameNotFound), retry_count=10)
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

    @retry(always_raise_list=(DocumentExistsException, CollectionNameNotFound), retry_count=10)
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

    @retry_a(always_raise_list=(DocumentExistsException, CollectionNameNotFound), retry_count=10)
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

    @retry(retry_count=10, always_raise_list=(CollectionNameNotFound,))
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

    @retry_a(retry_count=10, always_raise_list=(CollectionNameNotFound,))
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

    @retry(retry_count=10, always_raise_list=(CollectionNameNotFound,))
    def cb_subdoc_multi_upsert_s(self, key_list, field, value_list, name="_default"):
        try:
            for n in range(len(key_list)):
                self.cb_subdoc_upsert_s(key_list[n], field, value_list[n], name=name)
        except Exception as err:
            raise CollectionSubdocUpsertError(f"multi upsert error: {err}")

    @retry_a(retry_count=10, always_raise_list=(CollectionNameNotFound,))
    async def cb_subdoc_multi_upsert_a(self, key_list, field, value_list, name="_default"):
        tasks = []
        for n in range(len(key_list)):
            tasks.append(asyncio.create_task(self.cb_subdoc_upsert_a(key_list[n], field, value_list[n], name=name)))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                raise result

    @retry(retry_count=10, always_raise_list=(CollectionNameNotFound,))
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

    @retry_a(retry_count=10, always_raise_list=(CollectionNameNotFound,))
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

    def query_sql_constructor_s(self, field=None, name="_default", where=None, value=None, sql=None):
        if not where and not sql and field:
            keyspace = self.db.keyspace_s(name)
            query = "SELECT " + field + " FROM " + keyspace + ";"
        elif not sql and field:
            keyspace = self.db.keyspace_s(name)
            query = "SELECT " + field + " FROM " + keyspace + " WHERE " + where + " = \"" + str(value) + "\";"
        elif sql:
            query = sql
        else:
            raise QueryArgumentsError("query: either field or sql argument is required")

        return query

    def query_sql_constructor_a(self, field=None, name="_default", where=None, value=None, sql=None):
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

    @retry(retry_count=10, always_raise_list=(QueryException, CollectionNameNotFound, QueryArgumentsError, IndexExistsError))
    def cb_query_s(self, field=None, name="_default", where=None, value=None, sql=None):
        query = ""
        try:
            contents = []
            query = self.query_sql_constructor_s(field, name, where, value, sql)
            result = self.db.cluster_s.query(query, QueryOptions(metrics=False, adhoc=True))
            for item in result:
                contents.append(item)
            return contents
        except CollectionNameNotFound:
            raise
        except CouchbaseException as err:
            error_class = decode_error_code(err.context.first_error_code, err.context.first_error_message)
            self.logger.debug(f"query: {query}")
            self.logger.debug(f"query error code {err.context.first_error_code} message {err.context.first_error_message}")
            raise error_class(err.context.first_error_message)
        except Exception as err:
            raise QueryError("{}: can not query {} from {}: {}".format(query, field, name, err))

    @retry_a(retry_count=10, always_raise_list=(QueryException, CollectionNameNotFound, QueryArgumentsError, IndexExistsError))
    async def cb_query_a(self, field=None, name="_default", where=None, value=None, sql=None):
        query = ""
        try:
            contents = []
            query = self.query_sql_constructor_a(field, name, where, value, sql)
            result = self.db.cluster_a.query(query, QueryOptions(metrics=False, adhoc=True))
            async for item in result:
                contents.append(item)
            return contents
        except CollectionNameNotFound:
            raise
        except CouchbaseException as err:
            error_class = decode_error_code(err.context.first_error_code, err.context.first_error_message)
            self.logger.debug(f"query: {query}")
            self.logger.debug(f"query error code {err.context.first_error_code} message {err.context.first_error_message}")
            raise error_class(err.context.first_error_message)
        except Exception as err:
            raise QueryError("{}: can not query {} from {}: {}".format(query, field, name, err))

    @retry(retry_count=10, always_raise_list=(DocumentNotFoundException, CollectionNameNotFound))
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

    @retry_a(retry_count=10, always_raise_list=(DocumentNotFoundException, CollectionNameNotFound))
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
