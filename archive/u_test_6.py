#!/usr/bin/env python3

import psutil
import warnings
from collections import Counter
import queue
from threading import Event, Lock
from couchbase.auth import PasswordAuthenticator
import couchbase.cluster
try:
    from couchbase.options import ClusterTimeoutOptions, QueryOptions, LockMode, ClusterOptions, TLSVerifyMode
except ImportError:
    from couchbase.cluster import ClusterTimeoutOptions, QueryOptions, ClusterOptions
    from couchbase.options import LockMode, TLSVerifyMode
from couchbase.exceptions import QueryIndexAlreadyExistsException, BucketAlreadyExistsException, RequestCanceledException
from couchbase.management.buckets import CreateBucketSettings, BucketType
try:
    from couchbase.management.options import CreateQueryIndexOptions
except ModuleNotFoundError:
    from couchbase.management.queries import CreateQueryIndexOptions
from datetime import timedelta
import argparse
import logging

error_count = 0


def check_open_files():
    p = psutil.Process()
    open_files = p.open_files()
    open_count = len(open_files)
    connections = p.connections()
    con_count = len(connections)
    num_fds = p.num_fds()
    children = p.children(recursive=True)
    num_children = len(children)
    print(f"open: {open_count} connections: {con_count} fds: {num_fds} children: {num_children} ", end="")
    status_list = []
    for c in connections:
        status_list.append(c.status)
    status_values = Counter(status_list).keys()
    status_count = Counter(status_list).values()
    for state, count in zip(status_values, status_count):
        print(f"{state}: {count} ", end="")
    print("")


class params(object):

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--ssl', action='store_true', help="Use SSL")
        parser.add_argument('--host', action='store', help="Hostname or IP address", default="127.0.0.1")
        parser.add_argument('--user', action='store', help="User Name", default="Administrator")
        parser.add_argument('--password', action='store', help="User Password", default="password")
        parser.add_argument('--bucket', action='store', help="Test Bucket", default="testrun")
        parser.add_argument('--cycles', action='store', help="Numer of iterations", type=int, default=100)
        self.args = parser.parse_args()

    @property
    def parameters(self):
        return self.args


class cbdb(object):

    def __init__(self, hostname, username, password, bucket, ssl=False):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cluster_services = ["n1ql", "index", "data"]
        if ssl:
            self.connectType = "couchbases://"
        else:
            self.connectType = "couchbase://"
        self.username = username
        self.password = password
        self.host = hostname
        self.bucket = bucket
        self.bm = None
        self.qim = None
        self.cluster = None
        self.collection = None
        self.q = queue.Queue()
        self.control = Event()
        self.lock = Lock()

    def __exit__(self):
        self.cluster_disconnect()

    def cluster_connect(self):
        auth = PasswordAuthenticator(self.username, self.password)
        timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=30),
                                         kv_timeout=timedelta(seconds=30),
                                         connect_timeout=timedelta(seconds=5),
                                         management_timeout=timedelta(seconds=5),
                                         resolve_timeout=timedelta(seconds=5))
        try:
            self.cluster = couchbase.cluster.Cluster.connect(self.connectType + self.host, ClusterOptions(auth,
                                                                                                          timeout_options=timeouts,
                                                                                                          lockmode=LockMode.WAIT,
                                                                                                          tls_verify=TLSVerifyMode.NO_VERIFY))
            self.bm = self.cluster.buckets()
            self.qim = self.cluster.query_indexes()
        except Exception as err:
            raise Exception(f"can not connect to cluster at {self.host}: {err}")

    def cluster_disconnect(self):
        if self.cluster:
            self.cluster.close()
            self.cluster = None

    def do_stuff(self):
        doc_count = 100
        document = {
            "id": 1,
            "data": "data",
            "one": "one",
            "two": "two",
            "three": "tree"
        }
        try:
            print("Creating bucket")
            self.bm.create_bucket(CreateBucketSettings(name=self.bucket,
                                                       bucket_type=BucketType.COUCHBASE,
                                                       ram_quota_mb=256))
        except BucketAlreadyExistsException:
            pass

        try:
            print("Creating index")
            self.qim.create_primary_index(self.bucket, CreateQueryIndexOptions(deferred=False, timeout=timedelta(seconds=120), num_replicas=0))
        except QueryIndexAlreadyExistsException:
            pass

        bucket = self.cluster.bucket(self.bucket)
        self.collection = bucket.default_collection()

        print(f"Upsert {doc_count} test docs")
        for n in range(doc_count):
            doc_id = n + 1
            upsert_doc = document
            upsert_doc["id"] = doc_id
            document_id = f"{self.bucket}:{doc_id}"
            while True:
                try:
                    self.collection.upsert(document_id, upsert_doc)
                    break
                except RequestCanceledException:
                    continue
                except Exception as err:
                    print(f"Error: {err}")
                    break

        print("Getting test docs")
        for n in range(doc_count):
            doc_id = n + 1
            document_id = f"{self.bucket}:{doc_id}"
            while True:
                try:
                    result = self.collection.get(document_id)
                    break
                except RequestCanceledException:
                    continue
                except Exception as err:
                    print(f"Error: {err}")
                    break


warnings.filterwarnings("ignore")
logging.basicConfig(filename='test_output.out', filemode='w', level=logging.DEBUG)
logger = logging.getLogger()
couchbase.configure_logging(logger.name, level=logger.level)
p = params()
options = p.parameters

try:
    for x in range(options.cycles):
        print(f"==> Attempt {x+1}:")
        cbtest = cbdb(options.host, options.user, options.password, options.bucket, options.ssl)
        cbtest.cluster_connect()
        cbtest.do_stuff()
        cbtest.cluster_disconnect()
        check_open_files()
        print("====> done <====")
except Exception as e:
    print("test: error: %s" % str(e))
    error_count += 1

if error_count > 0:
    print(f"Error count: {error_count}")
else:
    print("Success.")
