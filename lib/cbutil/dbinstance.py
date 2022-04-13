##
##

from .exceptions import *
from couchbase.cluster import QueryIndexManager

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

    def drop_bucket(self):
        self.bucket_name = None
        self.bucket_obj_s = None
        self.bucket_obj_a = None
        self.drop_scope()
        self.drop_collections()

    def drop_scope(self):
        self.scope_name = None
        self.scope_obj_s = None
        self.scope_obj_a = None
        self.drop_collections()

    def drop_collections(self):
        self.collections_s = {}
        self.collections_a = {}

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