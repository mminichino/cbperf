##
##

from .exceptions import (DNSLookupTimeout, IsCollectionException, CollectionWaitException, ScopeWaitException, BucketWaitException, BucketNotFound,
                         CollectionNotDefined, IndexNotReady, IndexNotFoundError, NodeUnreachable, CollectionCountException, CollectionNameNotFound,
                         CollectionCountError, NodeConnectionTimeout, NodeConnectionError, NodeConnectionFailed, CollectionSubdocUpsertError, QueryArgumentsError,
                         IndexExistsError, QueryEmptyException, decode_error_code, IndexStatError, BucketStatsError,
                         ClusterKVServiceError, ClusterHealthCheckError, ClusterInitError, ClusterQueryServiceError, ClusterViewServiceError)
from .retries import retry
import logging
import asyncio
import socket
import dns.resolver
import sys
import re
from enum import Enum
from datetime import timedelta
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterTimeoutOptions, QueryOptions, ClusterOptions, LockMode, TLSVerifyMode
from couchbase.cluster import Cluster
from couchbase.diagnostics import ServiceType, PingState
from couchbase.exceptions import HTTPException


class RunMode(Enum):
    Sync = 0
    Async = 1


class cb_common(object):

    def __init__(self, hostname: str, username: str, password: str, ssl=False, external=False):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.loop = asyncio.get_event_loop()
        self.loop.set_exception_handler(self.unhandled_exception)
        self._cluster = None
        self._bucket = None
        self._scope = None
        self._collection = None
        self.username = username
        self.password = password
        self.ssl = ssl
        self.rally_host_name = hostname
        self.rally_cluster_node = self.rally_host_name
        self.rally_dns_domain = False
        self.use_external_network = external
        self.external_network_present = False
        self.node_list = []
        self.srv_host_list = []
        self.all_hosts = []
        self.node_cycle = None
        self.cluster_info = None
        self.sw_version = None
        self.memory_quota = None
        self.cluster_services = []
        self.auth = PasswordAuthenticator(self.username, self.password)
        self.timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=30),
                                              kv_timeout=timedelta(seconds=30),
                                              connect_timeout=timedelta(seconds=5),
                                              management_timeout=timedelta(seconds=5),
                                              resolve_timeout=timedelta(seconds=5))

        if self.ssl:
            self.prefix = "https://"
            self.cb_prefix = "couchbases://"
            self.srv_prefix = "_couchbases._tcp."
            self.admin_port = "18091"
            self.node_port = "19102"
        else:
            self.prefix = "http://"
            self.cb_prefix = "couchbase://"
            self.srv_prefix = "_couchbase._tcp."
            self.admin_port = "8091"
            self.node_port = "9102"

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

    def unhandled_exception(self, loop, context):
        err = context.get("exception", context['message'])
        if isinstance(err, Exception):
            self.logger.error(f"unhandled exception: type: {err.__class__.__name__} msg: {err} cause: {err.__cause__}")
        else:
            self.logger.error(f"unhandled error: {err}")

    # def sync(self):
    #     self._mode = RunMode.Sync.value
    #     return self
    #
    # def a_sync(self):
    #     self._mode = RunMode.Async.value
    #     return self

    # @property
    # def cb_parameters(self):
    #     if self.ssl:
    #         return "?ssl=no_verify"
    #     else:
    #         return ""

    @property
    def cb_connect_string(self):
        return self.cb_prefix + self.rally_host_name

    @property
    def cb_network(self):
        if self.use_external_network:
            return 'external'
        else:
            return 'default'

    @property
    def get_memory_quota(self):
        return self.memory_quota

    @retry(retry_count=5)
    def is_reachable(self):
        resolver = dns.resolver.Resolver()
        resolver.timeout = 5
        resolver.lifetime = 10

        try:
            answer = resolver.resolve(self.srv_prefix + self.rally_host_name, "SRV")
            for srv in answer:
                record = {'hostname': str(srv.target).rstrip('.')}
                host_answer = resolver.resolve(record['hostname'], 'A')
                record['address'] = host_answer[0].address
                self.srv_host_list.append(record)
            self.rally_cluster_node = self.srv_host_list[0]['hostname']
            self.rally_dns_domain = True
        except dns.resolver.NXDOMAIN:
            pass
        except dns.exception.Timeout:
            raise DNSLookupTimeout(f"{self.srv_prefix + self.rally_host_name} lookup timeout")
        except Exception:
            raise

        try:
            self.check_node_connectivity(self.rally_cluster_node, self.admin_port)
        except (NodeConnectionTimeout, NodeConnectionError, NodeConnectionFailed) as err:
            raise NodeUnreachable(f"can not connect to node {self.rally_cluster_node}: {err}")

        return True

    @retry(retry_count=5)
    def check_node_connectivity(self, hostname, port):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex((hostname, int(port)))
            sock.close()
        except socket.timeout:
            raise NodeConnectionTimeout(f"timeout connecting to {hostname}:{port}")
        except socket.error as err:
            raise NodeConnectionError(f"error connecting to {hostname}:{port}: {err}")

        if result == 0:
            return True
        else:
            raise NodeConnectionFailed(f"node {hostname}:{port} unreachable")

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

    @retry()
    def cluster_health_check(self, output=False, restrict=True, noraise=False, extended=False):
        try:
            cluster = Cluster(self.cb_connect_string, ClusterOptions(self.auth,
                                                                     timeout_options=self.timeouts,
                                                                     lockmode=LockMode.WAIT,
                                                                     tls_verify=TLSVerifyMode.NO_VERIFY))
            result = cluster.ping()
        except SystemError as err:
            if isinstance(err.__cause__, HTTPException) and err.__cause__.error_code == 1049:
                self.use_external_network = not self.use_external_network
                raise ClusterHealthCheckError("HTTP unknown host: {}".format(err.__cause__))
            else:
                raise ClusterHealthCheckError("HTTP error: {}".format(err))
        except Exception as err:
            raise ClusterHealthCheckError("cluster health check error: {}".format(err))

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
                            raise ClusterKVServiceError("{} KV service not ok".format(self.cb_connect_string))
                        elif endpoint == ServiceType.Query:
                            raise ClusterQueryServiceError("{} query service not ok".format(self.cb_connect_string))
                        elif endpoint == ServiceType.View:
                            raise ClusterViewServiceError("{} view service not ok".format(self.cb_connect_string))
                        else:
                            raise ClusterHealthCheckError("{} service {} not ok".format(self.cb_connect_string, endpoint.value))

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
                    raise ClusterQueryServiceError(f"query service test error: {err}")

    def print_host_map(self):
        if self.rally_dns_domain:
            print("Name %s is a domain with SRV records:" % self.rally_host_name)
            for record in self.srv_host_list:
                print(" => %s (%s)" % (record['hostname'], record['address']))

        print("Cluster Host List:")
        for i, record in enumerate(self.cluster_info['nodes']):
            if 'alternateAddresses' in record:
                ext_host_name = record['alternateAddresses']['external']['hostname']
                ext_port_list = record['alternateAddresses']['external']['ports']
            else:
                ext_host_name = None
                ext_port_list = None
            host_name = record['configuredHostname']
            version = record['version']
            ostype = record['os']
            services = ','.join(record['services'])
            print(" [%02d] %s" % (i+1, host_name), end=' ')
            if ext_host_name:
                print("[external]> %s" % ext_host_name, end=' ')
            if ext_port_list:
                for key in ext_port_list:
                    print("%s:%s" % (key, ext_port_list[key]), end=' ')
            print("[Services] %s [version] %s [platform] %s" % (services, version, ostype))
