##
##

from .exceptions import (IsCollectionException, CollectionWaitException, ScopeWaitException, BucketWaitException, BucketNotFound,
                         IndexNotReady, IndexNotFoundError, CollectionCountException, CollectionNameNotFound,
                         CollectionCountError, QueryArgumentsError,
                         IndexExistsError, QueryEmptyException, decode_error_code, IndexStatError, BucketStatsError,
                         ClusterInitError)
from .httpexceptions import HTTPNotImplemented
from .retries import retry
from .cbdebug import cb_debug
from .httpsessionmgr import api_session
from .cbcommon import cb_common, RunMode
from datetime import timedelta
try:
    from acouchbase.cluster import AsyncCluster
except ImportError:
    from acouchbase.cluster import Cluster as AsyncCluster
from couchbase.management.buckets import CreateBucketSettings, BucketType
from couchbase.management.collections import CollectionSpec
from couchbase.diagnostics import ServiceType
import couchbase.subdocument as SD
from couchbase.exceptions import (CouchbaseException, QueryIndexNotFoundException,
                                  DocumentNotFoundException, DocumentExistsException, QueryIndexAlreadyExistsException,
                                  BucketAlreadyExistsException, UnAmbiguousTimeoutException,
                                  BucketNotFoundException, WatchQueryIndexTimeoutException,
                                  ScopeAlreadyExistsException, CollectionAlreadyExistsException,
                                  CollectionNotFoundException)
import asyncio
from itertools import cycle
import json
try:
    from couchbase.options import ClusterTimeoutOptions, QueryOptions, LockMode, ClusterOptions, WaitUntilReadyOptions, TLSVerifyMode
except ImportError:
    from couchbase.cluster import ClusterTimeoutOptions, QueryOptions, ClusterOptions, WaitUntilReadyOptions
    from couchbase.options import LockMode, TLSVerifyMode
try:
    from couchbase.management.options import (CreateQueryIndexOptions, CreatePrimaryQueryIndexOptions, WatchQueryIndexOptions,
                                              DropPrimaryQueryIndexOptions, DropQueryIndexOptions)
except ModuleNotFoundError:
    from couchbase.management.queries import (CreateQueryIndexOptions, CreatePrimaryQueryIndexOptions, WatchQueryIndexOptions,
                                              DropPrimaryQueryIndexOptions, DropQueryIndexOptions)


class cb_connect_a(cb_common):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._mode = RunMode.Async.value
        self._mode_str = RunMode(self._mode).name

    def __exit__(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.disconnect())

    async def init(self):
        try:
            self.is_reachable()
            await self.connect()
            await self._cluster.wait_until_ready(timedelta(seconds=3),
                                                 WaitUntilReadyOptions(
                                                 service_types=[ServiceType.KeyValue,
                                                                ServiceType.Query,
                                                                ServiceType.Management]))

            s = api_session(self.username, self.password)
            s.set_host(self.rally_cluster_node, self.ssl, self.admin_port)
            self.cluster_info = s.api_get('/pools/default').json()

            ping_result = await self._cluster.ping()
            result_json = ping_result.as_json()
            result_dict = json.loads(result_json)

            for item in result_dict["services"]["mgmt"]:
                remote = item["remote"].split(":")[0]
                self.all_hosts.append(remote)

            info_result = await self._cluster.cluster_info()

            self.sw_version = info_result.server_version
            self.memory_quota = self.cluster_info['memoryQuota']

            self.node_cycle = cycle(self.all_hosts)

            return self
        except UnAmbiguousTimeoutException as err:
            print(f"Cluster not ready: {err}")
        except Exception as err:
            raise ClusterInitError(f"cluster not reachable at {self.rally_host_name}: {err}")

    @retry()
    async def connect(self):
        self.logger.debug(f"connect [{self._mode_str}]: connect string {self.cb_connect_string}")
        cluster = AsyncCluster(self.cb_connect_string, ClusterOptions(self.auth,
                                                                      timeout_options=self.timeouts,
                                                                      lockmode=LockMode.WAIT,
                                                                      tls_verify=TLSVerifyMode.NO_VERIFY))
        result = await cluster.on_connect()
        self._cluster = cluster
        return True

    @retry()
    async def bucket(self, name):
        self.logger.debug(f"bucket [{self._mode_str}]: connecting bucket {name}")
        if self._cluster:
            self._bucket = self._cluster.bucket(name)
            await self._bucket.on_connect()
        else:
            self._bucket = None

    @retry()
    async def scope(self, name="_default"):
        self.logger.debug(f"scope [{self._mode_str}]: connecting scope {name}")
        if self._bucket:
            self.logger.debug(f"scope [{RunMode(self._mode).name}]: connecting scope {name}")
            self._scope = self._bucket.scope(name)
        else:
            self._scope = None

    @retry()
    async def collection(self, name="_default"):
        self.logger.debug(f"collection [{self._mode_str}]: connecting collection {name}")
        if self._scope:
            self.logger.debug(f"collection [{RunMode(self._mode).name}]: connecting collection {name}")
            self._collection = self._scope.collection(name)
        else:
            self._collection = None

    async def scope_list(self):
        cm = self._bucket.collections()
        return await cm.get_all_scopes()

    @retry(factor=0.5)
    async def is_bucket(self, bucket: str) -> bool:
        try:
            hostname = next(self.node_cycle)
            s = api_session(self.username, self.password)
            s.set_host(hostname, self.ssl, self.admin_port)
            results = s.api_get(f"/pools/default/buckets/{bucket}")
            return True
        except HTTPNotImplemented:
            return False

    @retry(always_raise_list=(AttributeError,))
    async def is_scope(self, scope):
        try:
            return next((s for s in await self.scope_list() if s.name == scope), None)
        except AttributeError:
            return None

    @retry(always_raise_list=(AttributeError,))
    async def is_collection(self, collection):
        try:
            scope_spec = next((s for s in await self.scope_list() if s.name == self._scope.name), None)
            if not scope_spec:
                raise IsCollectionException(f"is_collection: no scope configured")
            return next((c for c in scope_spec.collections if c.name == collection), None)
        except AttributeError:
            return None

    @retry()
    async def collection_wait(self, collection):
        if not await self.is_collection(collection):
            raise CollectionWaitException(f"waiting: collection {collection} does not exist")

    @retry()
    async def scope_wait(self, scope):
        if not await self.is_scope(scope):
            raise ScopeWaitException(f"waiting: scope {scope} does not exist")

    @retry()
    async def bucket_wait(self, bucket: str, count: int = 0):
        try:
            bucket_stats = await self.bucket_stats(bucket)
            if bucket_stats['itemCount'] < count:
                raise BucketWaitException(f"item count {bucket_stats['itemCount']} less than {count}")
        except Exception as err:
            raise BucketWaitException(f"bucket_wait: error: {err}")

    @retry()
    async def create_bucket(self, name, quota=256):
        self.logger.debug(f"create_bucket [{self._mode_str}]: create bucket {name}")
        try:
            bm = self._cluster.buckets()
            await bm.create_bucket(CreateBucketSettings(name=name,
                                                        bucket_type=BucketType.COUCHBASE,
                                                        ram_quota_mb=quota))
        except BucketAlreadyExistsException:
            pass
        await self.bucket(name)

    @retry()
    async def drop_bucket(self, name):
        self.logger.debug(f"drop_bucket [{self._mode_str}]: drop bucket {name}")
        try:
            bm = self._cluster.buckets()
            await bm.drop_bucket(name)
        except BucketNotFoundException:
            pass

    @retry()
    async def create_scope(self, name):
        self.logger.debug(f"create_scope [{self._mode_str}]: create scope {name}")
        try:
            cm = self._bucket.collections()
            await cm.create_scope(name)
        except ScopeAlreadyExistsException:
            pass
        await self.scope(name)

    @retry()
    async def create_collection(self, name):
        self.logger.debug(f"create_collection [{self._mode_str}]: create collection {name}")
        try:
            collection_spec = CollectionSpec(name, scope_name=self._scope.name)
            cm = self._bucket.collections()
            await cm.create_collection(collection_spec)
        except CollectionAlreadyExistsException:
            pass
        await self.collection(name)

    @retry()
    async def drop_collection(self, name):
        self.logger.debug(f"drop_collection [{self._mode_str}]: drop collection {name}")
        try:
            collection_spec = CollectionSpec(name, scope_name=self._scope.name)
            cm = self._bucket.collections()
            await cm.drop_collection(collection_spec)
        except CollectionNotFoundException:
            pass

    @retry()
    async def collection_count(self, expect_count: int = 0) -> int:
        try:
            query = 'select count(*) as count from ' + self.keyspace + ';'
            result = await self.cb_query(sql=query)
            count: int = int(result[0]['count'])
            if expect_count > 0:
                if count < expect_count:
                    raise CollectionCountException(f"expect count {expect_count} but current count is {count}")
            return count
        except Exception as err:
            self.logger.error(f"collection_count: error occurred: {err}")
            raise CollectionCountError(f"can not get item count for {self.keyspace}: {err}")

    @retry()
    async def cb_get(self, key):
        try:
            document_id = self.construct_key(key)
            result = await self._collection.get(document_id)
            self.logger.debug(f"cb_get [{self._mode_str}]: {document_id}: cas {result.cas}")
            return result.content_as[dict]
        except DocumentNotFoundException:
            return None

    @retry()
    async def cb_upsert(self, key, document):
        try:
            document_id = self.construct_key(key)
            result = await self._collection.upsert(document_id, document)
            self.logger.debug(f"cb_upsert [{self._mode_str}]: {document_id}: cas {result.cas}")
            return result
        except DocumentExistsException:
            return None

    @retry()
    async def cb_subdoc_upsert(self, key, field, value):
        document_id = self.construct_key(key)
        result = await self._collection.mutate_in(document_id, [SD.upsert(field, value)])
        self.logger.debug(f"cb_subdoc_upsert [{self._mode_str}]: {document_id}: cas {result.cas}")
        return result.content_as[dict]

    @retry()
    async def cb_subdoc_multi_upsert(self, key_list, field, value_list):
        tasks = []
        for n in range(len(key_list)):
            tasks.append(self.loop.create_task(self.cb_subdoc_upsert(key_list[n], field, value_list[n])))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                raise result

    @retry()
    async def cb_subdoc_get(self, key, field):
        document_id = self.construct_key(key)
        result = await self._collection.lookup_in(document_id, [SD.get(field)])
        self.logger.debug(f"cb_subdoc_get [{self._mode_str}]: {document_id}: cas {result.cas}")
        return result.content_as[dict]

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

    @retry(always_raise_list=(CollectionNameNotFound, QueryArgumentsError, IndexExistsError, QueryIndexNotFoundException))
    async def cb_query(self, field=None, where=None, value=None, sql=None, empty_retry=False):
        query = self.query_sql_constructor(field, where, value, sql)
        contents = []
        try:
            self.logger.debug(f"cb_query [{self._mode_str}]: running query: {query}")
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

    @retry()
    async def cb_remove(self, key):
        try:
            document_id = self.construct_key(key)
            return await self._collection.remove(document_id)
        except DocumentNotFoundException:
            return None

    @retry()
    async def bucket_stats(self, bucket):
        try:
            hostname = next(self.node_cycle)
            s = api_session(self.username, self.password)
            s.set_host(hostname, self.ssl, self.admin_port)
            results = s.api_get(f"/pools/default/buckets/{bucket}")
            basic_stats = results.json()['basicStats']
            return basic_stats
        except Exception as err:
            raise BucketStatsError(f"can not get bucket {bucket} stats: {err}")

    @retry()
    async def cb_create_primary_index(self, replica=0, timeout=480):
        if self._collection.name != '_default':
            index_options = CreatePrimaryQueryIndexOptions(deferred=False,
                                                           timeout=timedelta(seconds=timeout),
                                                           num_replicas=replica,
                                                           collection_name=self._collection.name,
                                                           scope_name=self._scope.name)
        else:
            index_options = CreatePrimaryQueryIndexOptions(deferred=False,
                                                           timeout=timedelta(seconds=timeout),
                                                           num_replicas=replica)
        self.logger.debug(f"cb_create_primary_index [{self._mode_str}]: creating primary index on {self._collection.name}")
        try:
            qim = self._cluster.query_indexes()
            await qim.create_primary_index(self._bucket.name, index_options)
        except QueryIndexAlreadyExistsException:
            pass

    @retry()
    async def cb_create_index(self, field, replica=0, timeout=480):
        if self._collection.name != '_default':
            index_options = CreateQueryIndexOptions(deferred=False,
                                                    timeout=timedelta(seconds=timeout),
                                                    num_replicas=replica,
                                                    collection_name=self._collection.name,
                                                    scope_name=self._scope.name)
        else:
            index_options = CreateQueryIndexOptions(deferred=False,
                                                    timeout=timedelta(seconds=timeout),
                                                    num_replicas=replica)
        self.logger.debug(f"cb_create_primary_index [{self._mode_str}]: creating index on {self._collection.name}")
        try:
            index_name = self.index_name(field)
            qim = self._cluster.query_indexes()
            await qim.create_index(self._bucket.name, index_name, [field], index_options)
        except QueryIndexAlreadyExistsException:
            pass

    @retry()
    async def cb_drop_primary_index(self, timeout=120):
        if self._collection.name != '_default':
            index_options = DropPrimaryQueryIndexOptions(timeout=timedelta(seconds=timeout),
                                                         collection_name=self._collection.name,
                                                         scope_name=self._scope.name)
        else:
            index_options = DropPrimaryQueryIndexOptions(timeout=timedelta(seconds=timeout))
        self.logger.debug(f"cb_drop_primary_index [{self._mode_str}]: dropping primary index on {self.collection_name}")
        try:
            qim = self._cluster.query_indexes()
            await qim.drop_primary_index(self._bucket.name, index_options)
        except QueryIndexNotFoundException:
            pass

    @retry()
    async def cb_drop_index(self, field, timeout=120):
        index_name = self.effective_index_name(field)
        if self._collection.name != '_default':
            index_options = DropQueryIndexOptions(timeout=timedelta(seconds=timeout),
                                                  collection_name=self._collection.name,
                                                  scope_name=self._scope.name)
        else:
            index_options = DropQueryIndexOptions(timeout=timedelta(seconds=timeout))
        self.logger.debug(f"cb_drop_index [{self._mode_str}]: drop index {index_name}")
        try:
            qim = self._cluster.query_indexes()
            await qim.drop_index(self._bucket.name, index_name, index_options)
        except QueryIndexNotFoundException:
            pass

    @retry()
    async def index_list_all(self):
        qim = self._cluster.query_indexes()
        index_list = await qim.get_all_indexes(self._bucket.name)
        return index_list

    @retry()
    async def is_index(self, field=None):
        index_name = self.effective_index_name(field)
        try:
            index_list = await self.index_list_all()
            for item in index_list:
                if index_name == '#primary':
                    if (item.collection_name == self.collection_name or item.bucket_name == self.collection_name) and item.name == '#primary':
                        return True
                elif item.name == index_name:
                    return True
        except Exception as err:
            raise IndexStatError("Could not get index status: {}".format(err))

        return False

    @retry(factor=0.5, allow_list=(IndexNotReady,))
    async def index_wait(self, field=None):
        record_count = await self.collection_count()
        try:
            await self.index_check(field=field, check_count=record_count)
        except Exception:
            raise IndexNotReady(f"index_wait: index not ready")

    async def get_index_key(self, field=None):
        index_name = self.effective_index_name(field)
        doc_key_field = 'meta().id'
        index_list = await self.index_list_all()

        for item in index_list:
            if item.name == index_name and (item.collection_name == self.collection_name or item.bucket_name == self.collection_name):
                if len(item.index_key) == 0:
                    return doc_key_field
                else:
                    return item.index_key[0]

        raise IndexNotFoundError(f"index {index_name} not found")

    @retry()
    async def index_check(self, field=None, check_count=0):
        try:
            query_field = await self.get_index_key(field)
        except Exception:
            raise

        query_text = f"SELECT {query_field} FROM {self.keyspace} WHERE TOSTRING({query_field}) LIKE \"%\" ;"
        result = await self.cb_query(sql=query_text)

        if check_count >= len(result):
            return True
        else:
            raise IndexNotReady(f"index_check: field: {field} count {check_count} len {len(result)}: index not ready")

    async def index_online(self, name=None, primary=False, timeout=480):
        if primary:
            indexes = []
            watch_options = WatchQueryIndexOptions(timeout=timedelta(seconds=timeout), watch_primary=True)
        else:
            indexes = [name]
            watch_options = WatchQueryIndexOptions(timeout=timedelta(seconds=timeout))
        try:
            qim = self._cluster.query_indexes()
            await qim.watch_indexes(self._bucket.name,
                                    indexes,
                                    watch_options)
        except WatchQueryIndexTimeoutException:
            raise IndexNotReady(f"Indexes not build within {timeout} seconds...")

    @retry(factor=0.5, allow_list=(IndexNotReady,))
    async def index_list(self):
        return_list = {}
        try:
            index_list = await self.index_list_all()
            for item in index_list:
                if item.collection_name == self.collection_name or item.bucket_name == self.collection_name:
                    return_list[item.name] = item.state
            return return_list
        except Exception as err:
            raise IndexNotReady(f"index_list: bucket {self._bucket.name} error: {err}")

    @retry(factor=0.5, allow_list=(IndexNotReady,))
    async def delete_wait(self, field=None):
        if await self.is_index(field=field):
            raise IndexNotReady(f"delete_wait: index still exists")

    async def disconnect(self):
        if self._cluster:
            await self._cluster.close()
            self._cluster = None
