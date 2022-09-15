##
##

from .sessionmgr import cb_session
from .exceptions import (ClusterConnectException, IsCollectionException, CollectionWaitException, ScopeWaitException, BucketWaitException, BucketNotFound,
                         CollectionNotDefined, IndexNotReady, IndexNotFoundError, CollectionRemoveError, CollectionCountException, CollectionNameNotFound,
                         CollectionCountError, CollectionGetError, CollectionUpsertError, CollectionSubdocUpsertError, CollectionSubdocGetError, QueryArgumentsError,
                         IndexExistsError, QueryEmptyException, decode_error_code, IndexStatError, BucketStatsError, ClusterInitError)
from .httpexceptions import HTTPNotImplemented
from .retries import RunMode, retry_a, retry_s, retry, MODE_SYNC, MODE_ASYNC
from .dbinstance import db_instance
from .cbdebug import cb_debug
from .httpsessionmgr import api_session
from datetime import timedelta
import concurrent.futures
try:
    from acouchbase.cluster import AsyncCluster
except ImportError:
    from acouchbase.cluster import Cluster as AsyncCluster
from couchbase.cluster import Cluster
from couchbase.management.buckets import CreateBucketSettings, BucketType
from couchbase.management.collections import CollectionSpec
from couchbase.auth import PasswordAuthenticator
import couchbase.subdocument as SD
from couchbase.exceptions import (CouchbaseException, QueryIndexNotFoundException,
                                  DocumentNotFoundException, DocumentExistsException, QueryIndexAlreadyExistsException,
                                  BucketAlreadyExistsException, HTTPException,
                                  BucketNotFoundException, WatchQueryIndexTimeoutException,
                                  ScopeAlreadyExistsException, CollectionAlreadyExistsException,
                                  CollectionNotFoundException)
import asyncio
import logging
import re
from typing import Callable
from functools import wraps, partial
import time
import os
try:
    from couchbase.options import ClusterTimeoutOptions, QueryOptions, LockMode, ClusterOptions
except ImportError:
    from couchbase.cluster import ClusterTimeoutOptions, QueryOptions, ClusterOptions
    from couchbase.options import LockMode
try:
    from couchbase.management.options import CreateQueryIndexOptions, CreatePrimaryQueryIndexOptions, WatchQueryIndexOptions
except ModuleNotFoundError:
    from couchbase.management.queries import CreateQueryIndexOptions, CreatePrimaryQueryIndexOptions, WatchQueryIndexOptions


class cb_connect(cb_session):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.loop = asyncio.get_event_loop()
        self.loop.set_exception_handler(self.unhandled_exception)
        self.db = db_instance()
        self._mode = RunMode.Sync.value
        self._cluster_a = None
        self._cluster_s = None
        self._cluster = None
        self._bucket_a = None
        self._bucket_s = None
        self._bucket = None
        self._scope = None
        self._collection = None
        self.auth = PasswordAuthenticator(self.username, self.password)
        self.timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=360), kv_timeout=timedelta(seconds=360))

    def construct_key(self, key):
        if type(key) == int or str(key).isdigit():
            if self._collection.name != "_default":
                return self._collection.name + ':' + str(key)
            else:
                return self._bucket.name + ':' + str(key)
        else:
            return key

    @property
    def keyspace(self):
        if self._scope.name != "_default" or self._collection.name != "_default":
            return self._bucket.name + '.' + self._scope.name + '.' + self._collection.name
        else:
            return self._bucket.name

    @property
    def collection_name(self):
        if self._collection.name == "_default":
            return self._bucket.name
        else:
            return self._collection.name

    def write_log(self, message: str, level: int = 2) -> None:
        debugger = cb_debug(self.__class__.__name__)
        logger = debugger.logger
        if level == 0:
            logger.debug(message)
        elif level == 1:
            logger.info(message)
        elif level == 2:
            logger.error(message)
        else:
            logger.critical(message)
        debugger.close()

    def unhandled_exception(self, loop, context):
        err = context.get("exception", context['message'])
        if isinstance(err, Exception):
            self.write_log(f"unhandled exception: type: {err.__class__.__name__} msg: {err} cause: {err.__cause__}")
        else:
            self.write_log(f"unhandled error: {err}")

    def sync(self):
        self._mode = RunMode.Sync.value
        return self

    def a_sync(self):
        self._mode = RunMode.Async.value
        return self

    def init(self):
        try:
            self.is_reachable()
            self.cluster_health_check(restrict=False)
            if self._mode == 0:
                self.connect_s()
            else:
                self.loop.run_until_complete(self.connect_a())
            return self
        except Exception as err:
            raise ClusterInitError(f"cluster not reachable at {self.rally_host_name}: {err}")

    def switch_mode(self, mode):
        self._mode = mode

    @retry(factor=0.5)
    async def connect_a(self):
        self.logger.debug(f"connect_a: connect string {self.cb_connect_string}")
        cluster = AsyncCluster(self.cb_connect_string, ClusterOptions(self.auth, timeout_options=self.timeouts, lockmode=LockMode.WAIT))
        result = await cluster.on_connect()
        self._cluster = cluster
        return True

    @retry(factor=0.5)
    def connect_s(self):
        self.logger.debug(f"connect_s: connect string {self.cb_connect_string}")
        cluster = Cluster(self.cb_connect_string, ClusterOptions(self.auth, timeout_options=self.timeouts, lockmode=LockMode.WAIT))
        self._cluster = cluster
        return True

    def bucket_s(self, name):
        if self._cluster:
            self._bucket = self._cluster.bucket(name)
        else:
            self._bucket = None

    async def bucket_a(self, name):
        if self._cluster:
            self._bucket = self._cluster.bucket(name)
            await self._bucket.on_connect()
        else:
            self._bucket = None

    @retry(factor=0.5)
    def bucket(self, name):
        self.logger.debug(f"bucket [{RunMode(self._mode).name}]: connecting bucket {name}")
        if self._mode == 0:
            self.bucket_s(name)
        else:
            self.loop.run_until_complete(self.bucket_a(name))

    # def scope_s(self, name="_default"):
    #     if self._bucket:
    #         cm = self._bucket.collections()
    #         scopes = cm.get_all_scopes()
    #         self._scope = next((s for s in scopes if s.name == name), None)
    #     else:
    #         self._scope = None
    #
    # async def scope_a(self, name="_default"):
    #     if self._bucket:
    #         cm = self._bucket.collections()
    #         scopes = await cm.get_all_scopes()
    #         self._scope = next((s for s in scopes if s.name == name), None)
    #     else:
    #         self._scope = None

    @retry(factor=0.5)
    def scope(self, name="_default"):
        if self._bucket:
            self.logger.debug(f"scope [{RunMode(self._mode).name}]: connecting scope {name}")
            self._scope = self._bucket.scope(name)
        else:
            self._scope = None

    # def collection_s(self, name="_default"):
    #     if self._scope:
    #         self._collection = self._scope.collection(name)
    #     else:
    #         self._collection = None
    #
    # async def collection_a(self, name="_default"):
    #     if self._scope:
    #         self._collection = self._scope.collection(name)
    #         await self._collection.on_connect()
    #     else:
    #         self._collection = None

    def collection(self, name="_default"):
        if self._scope:
            self.logger.debug(f"collection [{RunMode(self._mode).name}]: connecting collection {name}")
            self._collection = self._scope.collection(name)
        else:
            self._collection = None

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

    @retry_s(retry_count=10)
    def quick_connect_s(self, bucket, scope, collection):
        try:
            self.connect_s()
            self.bucket_s(bucket)
            self.scope_s(scope)
            self.collection_s(collection)
            return True
        except Exception as err:
            raise ClusterConnectException(f"quick connect error: {err}")

    @retry(factor=0.5)
    def is_bucket(self, bucket):
        try:
            hostname = next(self.node_cycle)
            s = api_session(self.username, self.password)
            s.set_host(hostname, self.ssl, self.admin_port)
            results = s.api_get(f"/pools/default/buckets/{bucket}")
            return True
        except HTTPNotImplemented:
            raise BucketNotFound(f"bucket {bucket} not found")

    @retry_s(always_raise_list=(AttributeError,), retry_count=10)
    def is_scope(self, scope):
        try:
            return next((s for s in self.db.cm_s.get_all_scopes() if s.name == scope), None)
        except AttributeError:
            return None

    @retry_s(always_raise_list=(AttributeError,), retry_count=10)
    def is_collection(self, collection):
        try:
            scope_spec = next((s for s in self.db.cm_s.get_all_scopes() if s.name == self.db.scope_name), None)
            if not scope_spec:
                raise IsCollectionException(f"is_collection: no scope configured")
            return next((c for c in scope_spec.collections if c.name == collection), None)
        except AttributeError:
            return None

    @retry_s(retry_count=10)
    def collection_wait(self, collection):
        if not self.is_collection(collection):
            raise CollectionWaitException(f"waiting: collection {collection} does not exist")

    @retry_s(retry_count=10)
    def scope_wait(self, scope):
        if not self.is_scope(scope):
            raise ScopeWaitException(f"waiting: scope {scope} does not exist")

    @retry(factor=0.5)
    def bucket_wait(self, bucket: str, count: int = 0):
        try:
            bucket_stats = self.bucket_stats(bucket)
            if bucket_stats['itemCount'] < count:
                raise BucketWaitException(f"item count {bucket_stats['itemCount']} less than {count}")
        except Exception as err:
            raise BucketWaitException(f"bucket_wait: error: {err}")

    def create_bucket_s(self, name, quota=256):
        try:
            bm = self._cluster.buckets()
            bm.create_bucket(CreateBucketSettings(name=name,
                                                  bucket_type=BucketType.COUCHBASE,
                                                  ram_quota_mb=quota))
        except BucketAlreadyExistsException:
            pass

    async def create_bucket_a(self, name, quota=256):
        try:
            bm = self._cluster.buckets()
            await bm.create_bucket(CreateBucketSettings(name=name,
                                                        bucket_type=BucketType.COUCHBASE,
                                                        ram_quota_mb=quota))
        except BucketAlreadyExistsException:
            pass

    @retry()
    def create_bucket(self, name, quota=256):
        self.logger.debug(f"create_bucket [{RunMode(self._mode).name}]: create bucket {name}")
        if self._mode == 0:
            self.create_bucket_s(name, quota)
        else:
            self.loop.run_until_complete(self.create_bucket_a(name, quota))
        self.bucket(name)

    def drop_bucket_s(self, name):
        try:
            bm = self._cluster.buckets()
            bm.drop_bucket(name)
        except BucketNotFoundException:
            pass

    async def drop_bucket_a(self, name):
        try:
            bm = self._cluster.buckets()
            await bm.drop_bucket(name)
        except BucketNotFoundException:
            pass

    @retry()
    def drop_bucket(self, name):
        self.logger.debug(f"drop_bucket [{RunMode(self._mode).name}]: drop bucket {name}")
        if self._mode == 0:
            self.drop_bucket_s(name)
        else:
            self.loop.run_until_complete(self.drop_bucket_a(name))

    def create_scope_s(self, name):
        try:
            cm = self._bucket.collections()
            cm.create_scope(name)
        except ScopeAlreadyExistsException:
            pass

    async def create_scope_a(self, name):
        try:
            cm = self._bucket.collections()
            await cm.create_scope(name)
        except ScopeAlreadyExistsException:
            pass

    @retry()
    def create_scope(self, name):
        self.logger.debug(f"create_scope [{RunMode(self._mode).name}]: create scope {name}")
        if self._mode == 0:
            self.create_scope_s(name)
        else:
            self.loop.run_until_complete(self.create_scope_a(name))
        self.scope(name)

    def create_collection_s(self, name):
        try:
            collection_spec = CollectionSpec(name, scope_name=self._scope.name)
            cm = self._bucket.collections()
            cm.create_collection(collection_spec)
        except CollectionAlreadyExistsException:
            pass

    async def create_collection_a(self, name):
        try:
            collection_spec = CollectionSpec(name, scope_name=self._scope.name)
            cm = self._bucket.collections()
            await cm.create_collection(collection_spec)
        except CollectionAlreadyExistsException:
            pass

    @retry()
    def create_collection(self, name):
        self.logger.debug(f"create_collection [{RunMode(self._mode).name}]: create collection {name}")
        if self._mode == 0:
            self.create_collection_s(name)
        else:
            self.loop.run_until_complete(self.create_collection_a(name))
        self.collection(name)

    def drop_collection_s(self, name):
        try:
            collection_spec = CollectionSpec(name, scope_name=self._scope.name)
            cm = self._bucket.collections()
            cm.drop_collection(collection_spec)
        except CollectionNotFoundException:
            pass

    async def drop_collection_a(self, name):
        try:
            collection_spec = CollectionSpec(name, scope_name=self._scope.name)
            cm = self._bucket.collections()
            await cm.drop_collection(collection_spec)
        except CollectionNotFoundException:
            pass

    @retry()
    def drop_collection(self, name):
        self.logger.debug(f"drop_collection [{RunMode(self._mode).name}]: drop collection {name}")
        if self._mode == 0:
            self.drop_collection_s(name)
        else:
            self.loop.run_until_complete(self.drop_collection_a(name))

    @retry(factor=0.5)
    def collection_count(self, expect_count: int = 0) -> int:
        try:
            query = 'select count(*) as count from ' + self.keyspace + ';'
            result = self.cb_query(sql=query)
            count: int = int(result[0]['count'])
            if expect_count > 0:
                if count < expect_count:
                    raise CollectionCountException(f"expect count {expect_count} but current count is {count}")
            return count
        except Exception as err:
            self.logger.error(f"collection_count: error occurred: {err}")
            raise CollectionCountError(f"can not get item count for {self.keyspace}: {err}")

    def cb_get_s(self, document_id):
        try:
            result = self._collection.get(document_id)
            self.logger.debug(f"cb_get [{RunMode(self._mode).name}]: {document_id}: cas {result.cas}")
            return result.content_as[dict]
        except DocumentNotFoundException:
            return None

    async def cb_get_a(self, document_id):
        try:
            result = await self._collection.get(document_id)
            self.logger.debug(f"cb_get [{RunMode(self._mode).name}]: {document_id}: cas {result.cas}")
            return result.content_as[dict]
        except DocumentNotFoundException:
            return None

    @retry()
    def cb_get(self, key):
        self.logger.debug(f"cb_get [{RunMode(self._mode).name}]: key {key}")
        if self._collection:
            document_id = self.construct_key(key)
            if self._mode == 0:
                return self.cb_get_s(document_id)
            else:
                return self.cb_get_a(document_id)
        else:
            raise CollectionNotDefined(f"cb_upsert: connect to collection first")

    def cb_upsert_s(self, document_id, document):
        try:
            result = self._collection.upsert(document_id, document)
            self.logger.debug(f"cb_upsert [{RunMode(self._mode).name}]: {document_id}: cas {result.cas}")
            return result
        except DocumentExistsException:
            return None

    async def cb_upsert_a(self, document_id, document):
        try:
            result = await self._collection.upsert(document_id, document)
            self.logger.debug(f"cb_upsert [{RunMode(self._mode).name}]: {document_id}: cas {result.cas}")
            return result
        except DocumentExistsException:
            return None

    @retry()
    def cb_upsert(self, key, document):
        self.logger.debug(f"cb_upsert [{RunMode(self._mode).name}]: key {key}")
        if self._collection:
            document_id = self.construct_key(key)
            if self._mode == 0:
                return self.cb_upsert_s(document_id, document)
            else:
                return self.cb_upsert_a(document_id, document)
        else:
            raise CollectionNotDefined(f"cb_upsert: connect to collection first")

    def cb_subdoc_upsert_s(self, document_id, field, value):
        result = self._collection.mutate_in(document_id, [SD.upsert(field, value)])
        self.logger.debug(f"cb_subdoc_upsert [{RunMode(self._mode).name}]: {document_id}: cas {result.cas}")
        return result.content_as[dict]

    async def cb_subdoc_upsert_a(self, document_id, field, value):
        result = await self._collection.mutate_in(document_id, [SD.upsert(field, value)])
        self.logger.debug(f"cb_subdoc_upsert [{RunMode(self._mode).name}]: {document_id}: cas {result.cas}")
        return result.content_as[dict]

    @retry()
    def cb_subdoc_upsert(self, key, field, value):
        self.logger.debug(f"cb_subdoc_upsert [{RunMode(self._mode).name}]: key {key} field {field}")
        if self._collection:
            document_id = self.construct_key(key)
            if self._mode == 0:
                return self.cb_subdoc_upsert_s(document_id, field, value)
            else:
                return self.cb_subdoc_upsert_a(document_id, field, value)
        else:
            raise CollectionNotDefined(f"cb_upsert: connect to collection first")

    def cb_subdoc_multi_upsert_s(self, key_list, field, value_list):
        tasks = set()
        executor = concurrent.futures.ThreadPoolExecutor()
        for n in range(len(key_list)):
            tasks.add(executor.submit(self.cb_subdoc_upsert, key_list[n], field, value_list[n]))
        while tasks:
            done, tasks = concurrent.futures.wait(tasks, return_when=concurrent.futures.FIRST_COMPLETED)
            for task in done:
                try:
                    result = task.result()
                except Exception as err:
                    raise CollectionSubdocUpsertError(f"multi upsert error: {err}")

    async def cb_subdoc_multi_upsert_a(self, key_list, field, value_list):
        loop = asyncio.get_event_loop()
        tasks = []
        for n in range(len(key_list)):
            tasks.append(loop.create_task(self.cb_subdoc_upsert(key_list[n], field, value_list[n])))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                raise result

    @retry()
    def cb_subdoc_multi_upsert(self, key_list, field, value_list):
        self.logger.debug(f"cb_subdoc_multi_upsert [{RunMode(self._mode).name}]: field {field}")
        if self._mode == 0:
            self.cb_subdoc_multi_upsert_s(key_list, field, value_list)
        else:
            self.loop.run_until_complete(self.cb_subdoc_multi_upsert_a(key_list, field, value_list))

    def cb_subdoc_get_s(self, document_id, field):
        result = self._collection.lookup_in(document_id, [SD.get(field)])
        self.logger.debug(f"cb_subdoc_get [{RunMode(self._mode).name}]: {document_id}: cas {result.cas}")
        return result.content_as[dict]

    async def cb_subdoc_get_a(self, document_id, field):
        result = await self._collection.lookup_in(document_id, [SD.get(field)])
        self.logger.debug(f"cb_subdoc_get [{RunMode(self._mode).name}]: {document_id}: cas {result.cas}")
        return result.content_as[dict]

    @retry()
    def cb_subdoc_get(self, key, field):
        self.logger.debug(f"cb_upsert [{RunMode(self._mode).name}]: key {key}")
        if self._collection:
            document_id = self.construct_key(key)
            if self._mode == 0:
                return self.cb_subdoc_get_s(document_id, field)
            else:
                return self.cb_subdoc_get_a(document_id, field)
        else:
            raise CollectionNotDefined(f"cb_upsert: connect to collection first")

    def query_sql_constructor(self, field=None, where=None, value=None, sql=None):
        if not where and not sql and field:
            query = "SELECT " + field + " FROM " + self.keyspace + ";"
        elif not sql and field:
            query = "SELECT " + field + " FROM " + self.keyspace + " WHERE " + where + " = \"" + str(value) + "\";"
        elif sql:
            query = sql
        else:
            raise QueryArgumentsError("query: either field or sql argument is required")
        return query

    def cb_query_s(self, field, where, value, sql, empty_retry):
        query = ""
        try:
            contents = []
            query = self.query_sql_constructor(field, where, value, sql)
            self.logger.debug(f"cb_query [{RunMode(self._mode).name}]: running query: {query}")
            result = self._cluster.query(query, QueryOptions(metrics=False, adhoc=True))
            for item in result:
                contents.append(item)
            if empty_retry:
                if len(contents) == 0:
                    raise QueryEmptyException(f"query did not return any results")
            return contents
        except CouchbaseException as err:
            try:
                error_class = decode_error_code(err.context.first_error_code, err.context.first_error_message)
                self.logger.debug(f"query: {query}", cb_debug.DEBUG)
                self.logger.debug(f"query error code {err.context.first_error_code} message {err.context.first_error_message}", cb_debug.DEBUG)
                raise error_class(err.context.first_error_message)
            except AttributeError:
                raise err

    async def cb_query_a(self, field, where, value, sql, empty_retry):
        query = ""
        try:
            contents = []
            query = self.query_sql_constructor(field, where, value, sql)
            self.logger.debug(f"cb_query [{RunMode(self._mode).name}]: running query: {query}")
            result = self._cluster.query(query, QueryOptions(metrics=False, adhoc=True))
            async for item in result:
                contents.append(item)
            if empty_retry:
                if len(contents) == 0:
                    raise QueryEmptyException(f"query did not return any results")
            return contents
        except CouchbaseException as err:
            try:
                error_class = decode_error_code(err.context.first_error_code, err.context.first_error_message)
                self.logger.debug(f"query: {query}", cb_debug.DEBUG)
                self.logger.debug(f"query error code {err.context.first_error_code} message {err.context.first_error_message}", cb_debug.DEBUG)
                raise error_class(err.context.first_error_message)
            except AttributeError:
                raise err

    @retry(always_raise_list=(CollectionNameNotFound, QueryArgumentsError, IndexExistsError, QueryIndexNotFoundException))
    def cb_query(self, field=None, where=None, value=None, sql=None, empty_retry=False):
        self.logger.debug(f"cb_query [{RunMode(self._mode).name}]: called")
        if self._collection:
            if self._mode == 0:
                return self.cb_query_s(field, where, value, sql, empty_retry)
            else:
                return self.cb_query_a(field, where, value, sql, empty_retry)
        else:
            raise CollectionNotDefined(f"cb_upsert: connect to collection first")

    def cb_remove_s(self, document_id):
        try:
            return self._collection.remove(document_id)
        except DocumentNotFoundException:
            return None

    async def cb_remove_a(self, document_id):
        try:
            return await self._collection.remove(document_id)
        except DocumentNotFoundException:
            return None

    @retry()
    def cb_remove(self, key):
        self.logger.debug(f"cb_remove [{RunMode(self._mode).name}]: key {key}")
        if self._collection:
            document_id = self.construct_key(key)
            if self._mode == 0:
                return self.cb_remove_s(document_id)
            else:
                return self.cb_remove_a(document_id)
        else:
            raise CollectionNotDefined(f"cb_upsert: connect to collection first")

    @retry()
    def bucket_stats(self, bucket):
        try:
            hostname = next(self.node_cycle)
            s = api_session(self.username, self.password)
            s.set_host(hostname, self.ssl, self.admin_port)
            results = s.api_get(f"/pools/default/buckets/{bucket}")
            basic_stats = results.json()['basicStats']
            return basic_stats
        except Exception as err:
            raise BucketStatsError(f"can not get bucket {bucket} stats: {err}")

    def index_name(self, field):
        field = field.replace('.', '_')
        field = re.sub('^_*', '', field)

        if self._collection.name != '_default':
            name = self._collection.name + '_' + field + '_ix'
        else:
            name = self._bucket.name + '_' + field + '_ix'

        return name

    def effective_index_name(self, field=None):
        if field:
            index_name = self.index_name(field)
        else:
            index_name = "#primary"
        return index_name

    def cb_create_primary_index_s(self, replicas=0, timeout=120):
        if self._collection.name != '_default':
            index_options = CreatePrimaryQueryIndexOptions(deferred=False,
                                                           timeout=timedelta(seconds=timeout),
                                                           num_replicas=replicas,
                                                           collection_name=self._collection.name,
                                                           scope_name=self._scope.name)
        else:
            index_options = CreatePrimaryQueryIndexOptions(deferred=False,
                                                           timeout=timedelta(seconds=timeout),
                                                           num_replicas=replicas)
        self.logger.debug(f"cb_create_primary_index [{RunMode(self._mode).name}]: creating primary index on {self._collection.name}")
        try:
            qim = self._cluster.query_indexes()
            qim.create_primary_index(self._bucket.name, index_options)
        except QueryIndexAlreadyExistsException:
            pass

    async def cb_create_primary_index_a(self, replicas=0, timeout=120):
        if self._collection.name != '_default':
            index_options = CreatePrimaryQueryIndexOptions(deferred=False,
                                                           timeout=timedelta(seconds=timeout),
                                                           num_replicas=replicas,
                                                           collection_name=self._collection.name,
                                                           scope_name=self._scope.name)
        else:
            index_options = CreatePrimaryQueryIndexOptions(deferred=False,
                                                           timeout=timedelta(seconds=timeout),
                                                           num_replicas=replicas)
        self.logger.debug(f"cb_create_primary_index [{RunMode(self._mode).name}]: creating primary index on {self._collection.name}")
        try:
            qim = self._cluster.query_indexes()
            await qim.create_primary_index(self._bucket.name, index_options)
        except QueryIndexAlreadyExistsException:
            pass

    @retry()
    def cb_create_primary_index(self, replicas=0, timeout=120):
        self.logger.debug(f"cb_create_primary_index [{RunMode(self._mode).name}]: create primary index")
        if self._collection:
            if self._mode == 0:
                return self.cb_create_primary_index_s(replicas, timeout)
            else:
                return self.loop.run_until_complete(self.cb_create_primary_index_a(replicas, timeout))
        else:
            raise CollectionNotDefined(f"cb_create_primary_index: connect to collection first")

    def cb_create_index_s(self, field, replicas=0, timeout=120):
        if self._collection.name != '_default':
            index_options = CreateQueryIndexOptions(deferred=False,
                                                    timeout=timedelta(seconds=timeout),
                                                    num_replicas=replicas,
                                                    collection_name=self._collection.name,
                                                    scope_name=self._scope.name)
        else:
            index_options = CreateQueryIndexOptions(deferred=False,
                                                    timeout=timedelta(seconds=timeout),
                                                    num_replicas=replicas)
        self.logger.debug(f"cb_create_primary_index [{RunMode(self._mode).name}]: creating index on {self._collection.name}")
        try:
            index_name = self.index_name(field)
            qim = self._cluster.query_indexes()
            qim.create_index(self._bucket.name, index_name, [field], index_options)
        except QueryIndexAlreadyExistsException:
            pass

    async def cb_create_index_a(self, field, replicas=0, timeout=120):
        if self._collection.name != '_default':
            index_options = CreateQueryIndexOptions(deferred=False,
                                                    timeout=timedelta(seconds=timeout),
                                                    num_replicas=replicas,
                                                    collection_name=self._collection.name,
                                                    scope_name=self._scope.name)
        else:
            index_options = CreateQueryIndexOptions(deferred=False,
                                                    timeout=timedelta(seconds=timeout),
                                                    num_replicas=replicas)
        self.logger.debug(f"cb_create_primary_index [{RunMode(self._mode).name}]: creating index on {self._collection.name}")
        try:
            index_name = self.index_name(field)
            qim = self._cluster.query_indexes()
            await qim.create_index(self._bucket.name, index_name, [field], index_options)
        except QueryIndexAlreadyExistsException:
            pass

    @retry()
    def cb_create_index(self, field, replicas=0, timeout=120):
        self.logger.debug(f"cb_create_index [{RunMode(self._mode).name}]: create index on field {field}")
        if self._collection:
            if self._mode == 0:
                return self.cb_create_index_s(field, replicas, timeout)
            else:
                return self.loop.run_until_complete(self.cb_create_index_a(field, replicas, timeout))
        else:
            raise CollectionNotDefined(f"cb_create_index: connect to collection first")

    def index_list_all_s(self):
        qim = self._cluster.query_indexes()
        index_list = qim.get_all_indexes(self._bucket.name)
        return index_list

    async def index_list_all_a(self):
        qim = self._cluster.query_indexes()
        index_list = await qim.get_all_indexes(self._bucket.name)
        return index_list

    @retry()
    def index_list_all(self):
        self.logger.debug(f"index_list_all [{RunMode(self._mode).name}]: call")
        if self._collection:
            if self._mode == 0:
                return self.index_list_all_s()
            else:
                return self.loop.run_until_complete(self.index_list_all_a())
        else:
            raise CollectionNotDefined(f"index_list_all: connect to collection first")

    def is_index(self, field=None):
        index_name = self.effective_index_name(field)
        try:
            if self._collection.name == "_default":
                collection_name = self._bucket.name
            else:
                collection_name = self._collection.name
            index_list = self.index_list_all()
            for i in range(len(index_list)):
                if index_name == '#primary':
                    if index_list[i].collection_name == collection_name and index_list[i].name == '#primary':
                        return True
                elif index_list[i].name == index_name:
                    return True
        except Exception as err:
            raise IndexStatError("Could not get index status: {}".format(err))

        return False

    @retry(factor=0.5, allow_list=(IndexNotReady,))
    def index_wait(self, field=None):
        record_count = self.collection_count()
        try:
            self.index_check(field=field, check_count=record_count)
        except Exception:
            raise IndexNotReady(f"index_wait: index not ready")

    # def index_stats(self, name=None):
    #     if not name:
    #         bucket = self.db.bucket_name
    #     else:
    #         bucket = name
    #
    #     index_data = {}
    #     endpoint = '/api/v1/stats/' + bucket
    #     s = api_session(self.username, self.password)
    #     for node in self.all_hosts:
    #         s.set_host(node, self.ssl, self.node_port)
    #         try:
    #             response_json = s.api_get(endpoint).json()
    #         except HTTPNotImplemented:
    #             continue
    #         for key in response_json:
    #             index_name = key.split(':')[-1]
    #             index_object = key.split(':')[-2]
    #             if index_object not in index_data:
    #                 index_data[index_object] = {}
    #             if index_name not in index_data[index_object]:
    #                 index_data[index_object][index_name] = response_json[key]
    #
    #     return index_data

    def get_index_key(self, field=None):
        index_name = self.effective_index_name(field)
        doc_key_field = 'meta().id'
        index_list = self.index_list_all()

        for item in index_list:
            if item.name == index_name and item.keyspace == self.collection_name:
                if len(item.index_key) == 0:
                    return doc_key_field
                else:
                    return item.index_key[0]

        raise IndexNotFoundError(f"index {index_name} not found")

    def index_check(self, field=None, check_count=0):
        try:
            query_field = self.get_index_key(field)
        except Exception:
            raise

        query_text = f"SELECT {query_field} FROM {self.keyspace} WHERE TOSTRING({query_field}) LIKE \"%\" ;"
        result = self.cb_query(sql=query_text)

        if len(result) >= check_count and len(result) > 0:
            return True
        else:
            raise IndexNotReady(f"index_check: index not ready")

    def index_online_s(self, name=None, primary=False, timeout=120):
        if primary:
            watch_options = WatchQueryIndexOptions(timeout=timedelta(seconds=timeout), watch_primary=True)
        else:
            watch_options = WatchQueryIndexOptions(timeout=timedelta(seconds=timeout))
        try:
            qim = self._cluster.query_indexes()
            qim.watch_indexes(self._bucket.name,
                              [name],
                              watch_options)
        except WatchQueryIndexTimeoutException:
            raise IndexNotReady(f"Indexes not build within {timeout} seconds...")

    async def index_online_a(self, name=None, primary=False, timeout=120):
        if primary:
            watch_options = WatchQueryIndexOptions(timeout=timedelta(seconds=timeout), watch_primary=True)
        else:
            watch_options = WatchQueryIndexOptions(timeout=timedelta(seconds=timeout))
        try:
            qim = self._cluster.query_indexes()
            await qim.watch_indexes(self._bucket.name,
                              [name],
                              watch_options)
        except WatchQueryIndexTimeoutException:
            raise IndexNotReady(f"Indexes not build within {timeout} seconds...")

    @retry(factor=0.5, allow_list=(IndexNotReady,))
    def index_online(self, name=None, primary=False, timeout=120):
        self.logger.debug(f"index_online [{RunMode(self._mode).name}]: name: {name} primary {primary}")
        if self._collection:
            if self._mode == 0:
                return self.index_online_s(name, primary, timeout)
            else:
                return self.loop.run_until_complete(self.index_online_a(name, primary, timeout))
        else:
            raise CollectionNotDefined(f"cb_create_index: connect to collection first")

    @retry(factor=0.5, allow_list=(IndexNotReady,))
    def index_list(self):
        try:
            index_list = self.index_list_all()
            for item in index_list:
                if item.keyspace == self.collection_name:
                    index_list[item.id] = item.name
            return index_list
        except Exception as err:
            raise IndexNotReady(f"index_list: bucket {self._bucket.name} error: {err}")

    @retry(factor=0.5, allow_list=(IndexNotReady,))
    def delete_wait(self, field=None):
        if self.is_index(field=field):
            raise IndexNotReady(f"delete_wait: index still exists")
