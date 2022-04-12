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
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (CASMismatchException, CouchbaseException, CouchbaseTransientException,
                                  DocumentNotFoundException, DocumentExistsException, BucketDoesNotExistException,
                                  BucketAlreadyExistsException, DurabilitySyncWriteAmbiguousException,
                                  BucketNotFoundException, NetworkException, QueryErrorContext, HTTPException)
import logging
import json


class db_instance(object):

    def __init__(self):
        self.cluster_obj = None
        self.bm_obj = None
        self.qim_obj = None
        self.bucket_obj = None
        self.bucket_name = None
        self.scope_obj = None
        self.scope_name = None
        self.collections = {}

    def set_cluster(self, cluster):
        self.cluster_obj = cluster

    def set_bm(self, bm):
        self.bm_obj = bm

    def set_qim(self, qim):
        self.qim_obj = qim

    def set_bucket(self, name, bucket):
        self.bucket_name = name
        self.bucket_obj = bucket

    def set_scope(self, name, scope):
        self.scope_name = name
        self.scope_obj = scope

    def add_collection(self, name, collection):
        self.collections[name] = collection

    @property
    def cluster(self):
        return self.cluster_obj

    @property
    def bm(self):
        return self.bm_obj

    @property
    def qim(self):
        return self.qim_obj

    @property
    def bucket(self):
        return self.bucket_obj

    @property
    def scope(self):
        return self.scope_obj

    def collection(self, name):
        if name in self.collections:
            return self.collections[name]
        else:
            return None


class cb_connect(cb_session):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth = PasswordAuthenticator(self.username, self.password)
        self.timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=60), kv_timeout=timedelta(seconds=60))
        self.db = db_instance()

    @retry(allow_list=(NetworkException, CouchbaseTransientException))
    def connect_s(self):
        cluster = couchbase.cluster.Cluster(self.cb_connect_string,
                                            authenticator=self.auth,
                                            lockmode=LOCKMODE_NONE,
                                            timeout_options=self.timeouts)
        bucket_manager = cluster.buckets()
        index_manager = QueryIndexManager(cluster)
        self.db.set_cluster(cluster)
        self.db.set_bm(bucket_manager)
        self.db.set_qim(index_manager)

    @retry(allow_list=(NetworkException, CouchbaseTransientException))
    async def connect_a(self):
        cluster = acouchbase.cluster.Cluster(self.cb_connect_string,
                                             authenticator=self.auth,
                                             lockmode=LOCKMODE_NONE,
                                             timeout_options=self.timeouts)
        await cluster.on_connect()
        bucket_manager = cluster.buckets()
        index_manager = QueryIndexManager(cluster)
        self.db.set_cluster(cluster)
        self.db.set_bm(bucket_manager)
        self.db.set_qim(index_manager)

    @retry(always_raise_list=(BucketNotFoundException,), allow_list=(NetworkException, CouchbaseTransientException))
    def bucket_s(self, name):
        bucket = self.db.cluster.bucket(name)
        self.db.set_bucket(name, bucket)

    @retry(always_raise_list=(BucketNotFoundException,), allow_list=(NetworkException, CouchbaseTransientException))
    async def bucket_a(self, name):
        bucket = self.db.cluster.bucket(name)
        await bucket.on_connect()
        self.db.set_bucket(name, bucket)

    @retry(always_raise_list=(BucketDoesNotExistException,), allow_list=(NetworkException, CouchbaseTransientException))
    def is_bucket(self, name):
        try:
            return self.db.bm.get_bucket(name)
        except BucketDoesNotExistException:
            return None

    @retry(always_raise_list=(BucketAlreadyExistsException,), allow_list=(NetworkException, CouchbaseTransientException))
    def create_bucket(self, name, quota=256):
        try:
            self.db.bm.create_bucket(CreateBucketSettings(name=name,
                                                          bucket_type=BucketType.COUCHBASE,
                                                          ram_quota_mb=quota))
            return True
        except BucketAlreadyExistsException:
            return False
