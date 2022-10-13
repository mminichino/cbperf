#!/usr/bin/env python3

import sys
import json
import psutil
import traceback
import warnings
from collections import Counter
import queue
from queue import Empty
from threading import Event, Lock
import concurrent.futures
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

    def doc_feed(self, document, doc_count):
        for n in range(doc_count):
            upsert_doc = document
            upsert_doc_id = n + 1
            upsert_doc["id"] = upsert_doc_id
            self.q.put((upsert_doc_id, json.dumps(upsert_doc)))
        self.q.join()
        self.control.set()

    def id_feed(self, doc_count):
        for n in range(doc_count):
            get_doc_id = n + 1
            self.q.put(get_doc_id)
        self.q.join()
        self.control.set()

    def do_upsert(self, doc_id, doc):
        while True:
            try:
                self.collection.upsert(doc_id, doc)
                break
            except RequestCanceledException:
                continue

    def do_get(self, doc_id):
        while True:
            try:
                return self.collection.get(doc_id)
            except RequestCanceledException:
                continue

    def doc_upsert_worker(self):
        while True:
            try:
                doc_id, document = self.q.get(block=False)
                document_id = f"{self.bucket}:{doc_id}"
                self.do_upsert(document_id, json.loads(document))
                self.q.task_done()
            except Empty:
                if self.control.is_set():
                    break
                else:
                    continue
            except Exception:
                raise

    def doc_get_worker(self):
        while True:
            try:
                doc_id = self.q.get(block=False)
                document_id = f"{self.bucket}:{doc_id}"
                result = self.do_get(document_id)
                self.q.task_done()
                return result.content_as[dict]
            except Empty:
                if self.control.is_set():
                    break
                else:
                    continue
            except Exception:
                raise

    def task_wait(self, tasks):
        result_set = []
        while tasks:
            done, tasks = concurrent.futures.wait(tasks, return_when=concurrent.futures.FIRST_COMPLETED)
            for task in done:
                try:
                    result = task.result()
                    if type(result) == dict:
                        result_set.append(result)
                except Exception as err:
                    print(f"task error: {type(err).__name__}: {err}")
                    return
        return result_set

    def do_stuff(self):
        doc_count = 100
        threads = 64
        tasks = set()
        sub_tasks = set()
        executor = concurrent.futures.ThreadPoolExecutor()
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

        try:
            print(f"Upsert {doc_count} test docs")
            tasks.add(executor.submit(self.doc_feed, document, doc_count))
            for worker in range(threads):
                tasks.add(executor.submit(self.doc_upsert_worker))
            result_set = self.task_wait(tasks)

            print("Getting test docs")
            tasks.clear()
            result_set.clear()
            run_count = 0
            tasks.add(executor.submit(self.id_feed, doc_count))
            while True:
                sub_tasks.clear()
                for y in range(threads):
                    sub_tasks.add(executor.submit(self.doc_get_worker))
                partial_set = self.task_wait(sub_tasks)
                result_set.extend(partial_set)
                run_count += threads
                if run_count >= doc_count:
                    break
            self.task_wait(tasks)

        except AssertionError:
            _, _, tb = sys.exc_info()
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]
            raise Exception(f"do_stuff: assertion failed at line {line} statement {text}")
        except Exception as err:
            raise Exception(f"do_stuff: failed with {err}")


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