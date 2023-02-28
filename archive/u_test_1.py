#!/usr/bin/env python3

import sys
import os
import base64
import logging
import json
import inspect
import psutil
from requests.auth import AuthBase
import requests
import traceback
from typing import Callable
from functools import wraps
from collections import Counter
import queue
import multiprocessing
from queue import Empty
from threading import Event, Lock
from requests.adapters import HTTPAdapter, Retry
import concurrent.futures
from couchbase.auth import PasswordAuthenticator
import couchbase.cluster
from couchbase.diagnostics import ServiceType
from couchbase.diagnostics import PingState
from couchbase.options import ClusterOptions, ClusterTimeoutOptions, QueryOptions
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


class IndexDoesNotExistException(Exception):

    def __init__(self, message):
        super().__init__(message)


class httpException(Exception):

    def __init__(self, message):
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        self.message = "Error: {} in {} {} at line {}: {}".format(
            type(self).__name__, filename, function, line, message)
        super().__init__(self.message)


class MissingAuthKey(httpException):
    pass


class MissingSecretKey(httpException):
    pass


class MissingClusterName(httpException):
    pass


class HTTPException(httpException):
    pass


class GeneralError(httpException):
    pass


class NotAuthorized(httpException):
    pass


class HTTPForbidden(httpException):
    pass


class RequestValidationError(httpException):
    pass


class InternalServerError(httpException):
    pass


class ClusterNotFound(httpException):
    pass


class ConnectException(httpException):
    pass


class HTTPNotImplemented(httpException):
    pass


class basic_auth(AuthBase):

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def __call__(self, r):
        auth_hash = f"{self.username}:{self.password}"
        auth_bytes = auth_hash.encode('ascii')
        auth_encoded = base64.b64encode(auth_bytes)
        request_headers = {
            "Authorization": f"Basic {auth_encoded.decode('ascii')}",
        }
        r.headers.update(request_headers)
        return r


class api_session(object):
    HTTP = 0
    HTTPS = 1

    def __init__(self, username=None, password=None):
        self.username = username
        self.password = password
        self.logger = logging.getLogger(self.__class__.__name__)
        self.url_prefix = "http://127.0.0.1"
        self.session = requests.Session()
        retries = Retry(total=60,
                        backoff_factor=0.1)
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self._response = None

        if "HTTP_DEBUG_LEVEL" in os.environ:
            import http.client as http_client
            http_client.HTTPConnection.debuglevel = 1
            logging.basicConfig()
            self.debug_level = int(os.environ['HTTP_DEBUG_LEVEL'])
            requests_log = logging.getLogger("requests.packages.urllib3")
            if self.debug_level == 0:
                self.logger.setLevel(logging.DEBUG)
                requests_log.setLevel(logging.DEBUG)
            elif self.debug_level == 1:
                self.logger.setLevel(logging.INFO)
                requests_log.setLevel(logging.INFO)
            elif self.debug_level == 2:
                self.logger.setLevel(logging.ERROR)
                requests_log.setLevel(logging.ERROR)
            else:
                self.logger.setLevel(logging.CRITICAL)
                requests_log.setLevel(logging.CRITICAL)
            requests_log.propagate = True

    def check_status_code(self, code):
        self.logger.debug("API status code {}".format(code))
        if code == 200 or code == 201:
            return True
        elif code == 401:
            raise NotAuthorized("API: Unauthorized")
        elif code == 403:
            raise HTTPForbidden("API: Forbidden: Insufficient privileges")
        elif code == 404:
            raise HTTPNotImplemented("API: Not Found")
        elif code == 422:
            raise RequestValidationError("API: Request Validation Error")
        elif code == 500:
            raise InternalServerError("API: Server Error")
        else:
            raise Exception("Unknown API status code {}".format(code))

    def set_host(self, hostname, ssl=0, port=None):
        if ssl == api_session.HTTP:
            port_num = port if port else 80
            self.url_prefix = f"http://{hostname}:{port_num}"
        else:
            port_num = port if port else 443
            self.url_prefix = f"https://{hostname}:{port_num}"

    @property
    def response(self):
        return self._response

    def json(self):
        return json.loads(self._response)

    def http_get(self, endpoint, headers=None, verify=False):
        response = self.session.get(self.url_prefix + endpoint, headers=headers, verify=verify)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        self._response = response.text
        return self

    def http_post(self, endpoint, data=None, headers=None, verify=False):
        response = self.session.post(self.url_prefix + endpoint, data=data, headers=headers, verify=verify)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        self._response = response.text
        return self

    def api_get(self, endpoint):
        response = self.session.get(self.url_prefix + endpoint, auth=basic_auth(self.username, self.password), verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        self._response = response.text
        return self

    def api_post(self, endpoint, body):
        response = self.session.post(self.url_prefix + endpoint, auth=basic_auth(self.username, self.password), json=body, verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        self._response = response.text
        return self

    def api_put(self, endpoint, body):
        response = self.session.put(self.url_prefix + endpoint, auth=basic_auth(self.username, self.password), json=body, verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        self._response = response.text
        return self

    def api_delete(self, endpoint):
        response = self.session.delete(self.url_prefix + endpoint, auth=basic_auth(self.username, self.password), verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        self._response = response.text
        return self


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
        self.apiSession = api_session(self.username, self.password)
        self.apiSession.set_host(self.host, ssl, self.adminPort)

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

    def cluster_health_check(self, output=False, restrict=True, noraise=False, extended=False):
        cb_auth = PasswordAuthenticator(self.username, self.password)
        cb_timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=60), kv_timeout=timedelta(seconds=60))

        try:
            cluster = couchbase.cluster.Cluster.connect(self.connectType + self.host + self.urlOptions, ClusterOptions(cb_auth, timeout_options=cb_timeouts))
            result = cluster.ping()
        except Exception as err:
            print("cluster_health_check: Can not connect to cluster: %s" % str(e))
            sys.exit(1)

        endpoint: ServiceType
        for endpoint, reports in result.endpoints.items():
            for report in reports:
                if restrict and endpoint != ServiceType.KeyValue:
                    continue
                report_string = " {0}: {1} took {2} {3}".format(
                    endpoint.value,
                    report.remote,
                    report.latency,
                    report.state.value)
                if output:
                    print(report_string)
                    continue
                if not report.state == PingState.OK:
                    if noraise:
                        print(f"{endpoint.value} service not ok: {report.state}")
                        sys.exit(2)
                    else:
                        if endpoint == ServiceType.KeyValue:
                            raise Exception("KV service not ok")
                        elif endpoint == ServiceType.Query:
                            raise Exception("query service not ok")
                        elif endpoint == ServiceType.View:
                            raise Exception("view service not ok")
                        else:
                            raise Exception("service not ok")

        if output:
            print("Cluster Diagnostics:")
            diag_result = cluster.diagnostics()
            for endpoint, reports in diag_result.endpoints.items():
                for report in reports:
                    report_string = " {0}: {1} last activity {2} {3}".format(
                        endpoint.value,
                        report.remote,
                        report.last_activity,
                        report.state.value)
                    print(report_string)

        if extended:
            try:
                if 'n1ql' in self.cluster_services:
                    query = "select * from system:datastores ;"
                    result = cluster.query(query, QueryOptions(metrics=False, adhoc=True))
                    print(f"Datastore query ok: returned {sum(1 for i in result.rows())} records")
                if 'index' in self.cluster_services:
                    query = "select * from system:indexes ;"
                    result = cluster.query(query, QueryOptions(metrics=False, adhoc=True))
                    print(f"Index query ok: returned {sum(1 for i in result.rows())} records")
            except Exception as err:
                if noraise:
                    print(f"query service not ready: {err}")
                    sys.exit(3)
                else:
                    raise Exception(f"query service test error: {err}")

        # cluster.close()

    def bucket_count(self, bucket):
        try:
            r = self.apiSession.api_get(f"/pools/default/buckets/{bucket}")
            bucket_stats = r.json()['basicStats']
            return bucket_stats['itemCount']
        except Exception as err:
            raise Exception(f"can not get bucket {bucket} stats: {err}")

    @retry_f(factor=0.5)
    def wait_count(self, bucket, count):
        current = self.bucket_count(bucket)
        if current < count:
            raise Exception(f"item count {current} less than {count}")

    def is_index(self, index, index_list, keyspace):
        for i in range(len(index_list)):
            if index == '#primary':
                if index_list[i].collection_name == keyspace and index_list[i].name == '#primary':
                    return True
            elif index_list[i].name == index:
                return True
        raise IndexDoesNotExistException(f"index {index} not found for keyspace {keyspace}")

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
                except Exception as err:
                    raise Exception(f"task error: {err}")
        return result_set

    def do_stuff(self):
        doc_count = 100
        threads = 64
        tasks = set()
        sub_tasks = set()
        executor = concurrent.futures.ThreadPoolExecutor()
        index_name = "test_ix_id"
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
            self.wait_count(self.bucket, doc_count)

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
            result_set.sort(key=lambda i: (int(i["id"])))
            for n, doc in zip(range(doc_count), result_set):
                assert doc["id"] == n + 1

            print("Getting collection doc count")
            result_list = self.doc_query_worker(queryTextA)
            assert result_list[0]["count"] == 100

            print("Getting field from test doc")
            result_list = self.doc_query_worker(queryTextB)
            result_list.sort(key=lambda i: (int(i["id"])))
            for n, doc in zip(range(doc_count), result_list):
                assert doc["id"] == n + 1

            print("API call test")
            results = self.apiSession.api_get('/pools/default')
            assert results.json()["name"] == "default"
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
        cbtest.cluster_health_check()
        check_open_files()
        print("====> done <====")
except Exception as e:
    print("test: error: %s" % str(e))
    error_count += 1

if error_count > 0:
    print(f"Error count: {error_count}")
else:
    print("Success.")
