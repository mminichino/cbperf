#!/usr/bin/env python3

import sys
import os
import base64
import logging
import json
import inspect
import socket
from requests.auth import AuthBase
import requests
from requests.adapters import HTTPAdapter, Retry
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterTimeoutOptions
import couchbase.cluster
from couchbase.diagnostics import ServiceType
from couchbase.diagnostics import PingState
from couchbase.cluster import Cluster, QueryOptions
from couchbase.options import ClusterOptions, ClusterTimeoutOptions
from datetime import timedelta
import time
import argparse

error_count = 0


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

        try:
            response_data = json.loads(response.text)
        except ValueError:
            response_data = response.text
        return response_data

    def api_post(self, endpoint, body):
        response = self.session.post(self.url_prefix + endpoint, auth=basic_auth(self.username, self.password), json=body, verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        try:
            response_data = json.loads(response.text)
        except ValueError:
            response_data = response.text
        return response_data

    def api_put(self, endpoint, body):
        response = self.session.put(self.url_prefix + endpoint, auth=basic_auth(self.username, self.password), json=body, verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        try:
            response_data = json.loads(response.text)
        except ValueError:
            response_data = response.text
        return response_data

    def api_delete(self, endpoint):
        response = self.session.delete(self.url_prefix + endpoint, auth=basic_auth(self.username, self.password), verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        try:
            response_data = json.loads(response.text)
        except ValueError:
            response_data = response.text
        return response_data


class params(object):

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--ssl', action='store_true')
        parser.add_argument('--close', action='store_true')
        self.args = parser.parse_args()
        self.sslFlag = self.args.ssl
        self.closeFlag = self.args.close

class cbdb(object):

    def __init__(self, ssl=False):
        self.cluster_services = ["n1ql", "index", "data"]
        if ssl:
            self.connectType = "couchbases://"
            self.urlOptions = "?ssl=no_verify"
            self.adminPort = "18091"
        else:
            self.connectType = "couchbase://"
            self.urlOptions = ""
            self.adminPort = "8091"
        self.username = "Administrator"
        self.password = "password"
        self.host = "10.28.1.48"
        self.bucket = "testrun"

    def syncDataConnect(self):
        auth = PasswordAuthenticator(self.username, self.password)
        timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=4800), kv_timeout=timedelta(seconds=4800))
        retries = 0
        while True:
            try:
                cluster = couchbase.cluster.Cluster.connect(self.connectType + self.host + self.urlOptions, authenticator=auth, timeout_options=timeouts)
                # bucket = cluster.bucket(self.bucket)
                return cluster
            except Exception as e:
                if retries == 10:
                    print("dataConnect: Can not connect to cluster: %s" % str(e))
                    sys.exit(1)
                else:
                    retries += 1
                    time.sleep(0.1 * retries)
                    continue

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


options = params()
cbtest = cbdb(options.sslFlag)

try:
    for x in range(4):
        print(f"==> Attempt {x+1}:")
        s = api_session(cbtest.username, cbtest.password)
        s.set_host(cbtest.host, options.sslFlag, cbtest.adminPort)
        results = s.api_get('/pools/default')
        cluster = cbtest.syncDataConnect()
        cbtest.cluster_health_check(output=True, restrict=False)
        if options.closeFlag:
            cluster.close()
except Exception as e:
    print("test: error: %s" % str(e))
    error_count += 1

if error_count > 0:
    print(f"Error count: {error_count}")
else:
    print("Success.")
