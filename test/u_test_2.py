#!/usr/bin/env python3

import sys
import json
import psutil
import traceback
from typing import Callable
from functools import wraps
from collections import Counter
import queue
from queue import Empty
from threading import Event, Lock
import concurrent.futures
from couchbase.auth import PasswordAuthenticator
import couchbase.cluster
from couchbase.options import ClusterTimeoutOptions, QueryOptions
from couchbase.exceptions import QueryIndexAlreadyExistsException, BucketAlreadyExistsException
from couchbase.management.buckets import CreateBucketSettings, BucketType
from couchbase.management.options import CreateQueryIndexOptions
from datetime import timedelta
import time
import argparse

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


def retry_f(retry_count=10,
            factor=0.01,
            allow_list=None,
            always_raise_list=None
            ) -> Callable:
    def retry_handler(func):
        @wraps(func)
        def f_wrapper(*args, **kwargs):
            for retry_number in range(retry_count + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as err:
                    if always_raise_list and isinstance(err, always_raise_list):
                        raise

                    if allow_list and not isinstance(err, allow_list):
                        raise

                    if retry_number == retry_count:
                        raise

                    wait = factor
                    wait *= (2**(retry_number+1))
                    time.sleep(wait)
        return f_wrapper
    return retry_handler


class params(object):

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--ssl', action='store_true')
        parser.add_argument('--host', action='store', default="127.0.0.1")
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
        self.cluster_services = ["n1ql", "index", "data"]
        if ssl:
            self.connectType = "couchbases://"
            self.urlOptions = "?ssl=no_verify"
            self.adminPort = "18091"
        else:
            self.connectType = "couchbase://"
            self.urlOptions = ""
            self.adminPort = "8091"
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

    @retry_f()
    def cluster_connect(self):
        auth = PasswordAuthenticator(self.username, self.password)
        timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=4800), kv_timeout=timedelta(seconds=4800))
        try:
            cluster = couchbase.cluster.Cluster.connect(self.connectType + self.host + self.urlOptions, authenticator=auth, timeout_options=timeouts)
            self.bm = cluster.buckets()
            self.qim = cluster.query_indexes()
            self.cluster = cluster
            return cluster
        except Exception as err:
            raise Exception(f"can not connect to cluster at {self.host}: {err}")

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

    @retry_f()
    def doc_upsert_worker(self):
        while True:
            try:
                doc_id, document = self.q.get(block=False)
                document_id = f"{self.bucket}:{doc_id}"
                self.collection.upsert(document_id, json.loads(document))
                self.q.task_done()
            except Empty:
                if self.control.is_set():
                    break
                else:
                    continue
            except Exception:
                raise

    @retry_f()
    def doc_get_worker(self):
        while True:
            try:
                doc_id = self.q.get(block=False)
                document_id = f"{self.bucket}:{doc_id}"
                result = self.collection.get(document_id)
                self.q.task_done()
                return result.content_as[dict]
            except Empty:
                if self.control.is_set():
                    break
                else:
                    continue
            except Exception:
                raise

    @retry_f()
    def doc_query_worker(self, query):
        result = self.cluster.query(query, QueryOptions(metrics=False, adhoc=True))
        result_list = list(result.rows())
        return result_list

    def task_wait(self, tasks):
        result_set = []
        while tasks:
            done, tasks = concurrent.futures.wait(tasks, return_when=concurrent.futures.FIRST_COMPLETED)
            for task in done:
                try:
                    result = task.result()
                    if type(result) == dict:
                        result_set.append(result)
                except Exception:
                    pass
        return result_set

    def do_stuff(self):
        doc_count = 100
        threads = 64
        tasks = set()
        sub_tasks = set()
        executor = concurrent.futures.ThreadPoolExecutor()
        queryTextA = "select count(*) as count from " + self.bucket + ";"
        queryTextB = "SELECT id FROM " + self.bucket + ";"
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

            print("Getting collection doc count")
            result_list = self.doc_query_worker(queryTextA)

            print("Getting field from test doc")
            result_list = self.doc_query_worker(queryTextB)

        except AssertionError:
            _, _, tb = sys.exc_info()
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]
            raise Exception(f"do_stuff: assertion failed at line {line} statement {text}")
        except Exception as err:
            raise Exception(f"do_stuff: failed with {err}")


p = params()
options = p.parameters
cbtest = cbdb(options.host, options.user, options.password, options.bucket, options.ssl)

try:
    for x in range(options.cycles):
        print(f"==> Attempt {x+1}:")
        cbtest.cluster_connect()
        cbtest.do_stuff()
        check_open_files()
        print("====> done <====")
except Exception as e:
    print("test: error: %s" % str(e))
    error_count += 1

if error_count > 0:
    print(f"Error count: {error_count}")
else:
    print("Success.")
