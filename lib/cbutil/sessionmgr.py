##
##

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
import logging
import socket
import dns.resolver
import traceback
from itertools import cycle
from datetime import timedelta
from couchbase.exceptions import (CouchbaseTransientException, TimeoutException, ProtocolException, HTTPException)
from couchbase.diagnostics import PingState
from couchbase.cluster import Cluster, QueryOptions, ClusterTimeoutOptions, QueryIndexManager
from couchbase.auth import PasswordAuthenticator
from couchbase.options import LOCKMODE_NONE
import lib.cbutil.exceptions
from .exceptions import *
from .retries import retry


class cb_session(object):

    def __init__(self, hostname: str, username: str, password: str, ssl=True, external=False, quick=False):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.rally_host_name = hostname
        self.rally_cluster_node = self.rally_host_name
        self.node_list = []
        self.srv_host_list = []
        self.all_hosts = []
        self.rally_dns_domain = False
        self.node_cycle = None
        self.cluster_info = None
        self.sw_version = None
        self.memory_quota = None
        self.username = username
        self.password = password
        self.ssl = ssl
        self.use_external_network = external
        self.external_network_present = False
        self.node_api_accessible = True
        self.quick_connect = quick

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

        if 'CB_PERF_DEBUG_LEVEL' in os.environ:
            try:
                debug_level = int(os.environ['CB_PERF_DEBUG_LEVEL'])
            except ValueError:
                raise CbUtilEnvironmentError("CB_PERF_DEBUG_LEVEL must be an integer")
            self.set_debug(level=debug_level)

        self.session = requests.Session()
        retries = Retry(total=60,
                        backoff_factor=0.1,
                        status_forcelist=[500, 501, 503])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

        self.init_cluster()

    def set_debug(self, level=1):
        if level == 0:
            self.logger.setLevel(logging.DEBUG)
        elif level == 1:
            self.logger.setLevel(logging.INFO)
        elif level == 2:
            self.logger.setLevel(logging.ERROR)
        else:
            self.logger.setLevel(logging.CRITICAL)

    def check_status_code(self, code):
        self.logger.debug("Couchbase API call status code {}".format(code))
        if code == 200:
            return True
        elif code == 401:
            raise NotAuthorized("Unauthorized: Insufficient privileges")
        elif code == 403:
            raise ForbiddenError("Forbidden")
        elif code == 404:
            raise NotFoundError("Not Found")
        else:
            raise Exception("Unknown API status code {}".format(code))

    @retry(allow_list=(DNSLookupTimeout, NodeUnreachable))
    def is_reachable(self):
        resolver = dns.resolver.Resolver()
        resolver.timeout = 15
        resolver.lifetime = 25

        try:
            answer = resolver.resolve(self.srv_prefix + self.rally_host_name, "SRV")
            for srv in answer:
                record = {'hostname': str(srv.target).rstrip('.')}
                host_answer = resolver.resolve(record['hostname'], 'A')
                record['address'] = host_answer[0].address
                self.srv_host_list.append(record)
            self.logger.info("rally name {} is a DNS domain".format(self.rally_host_name))
            self.rally_cluster_node = self.srv_host_list[0]['hostname']
            self.rally_dns_domain = True
        except dns.resolver.NXDOMAIN:
            self.logger.info("rally name {} is a node name".format(self.rally_host_name))
            pass
        except dns.exception.Timeout:
            raise DNSLookupTimeout("{} lookup timeout".format(self.srv_prefix + self.rally_host_name))
        except Exception:
            raise

        try:
            self.check_node_connectivity(self.rally_cluster_node, self.admin_port)
        except (NodeConnectionTimeout, NodeConnectionError, NodeConnectionFailed) as err:
            raise NodeUnreachable("can not connect to node {}: {}".format(self.rally_cluster_node, err))

        self.logger.info("initial connect node name: {}".format(self.rally_cluster_node))
        self.logger.debug("is_reachable: rally_host_name: {}".format(self.rally_host_name))
        self.logger.debug("is_reachable: rally_cluster_node: {}".format(self.rally_cluster_node))
        return True

    @retry(allow_list=(NodeConnectionTimeout, NodeConnectionError, NodeConnectionFailed))
    def check_node_connectivity(self, hostname, port):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            result = sock.connect_ex((hostname, int(port)))
            sock.close()
        except socket.timeout:
            raise NodeConnectionTimeout("timeout connecting to {}:{}".format(hostname, port))
        except socket.error as err:
            raise NodeConnectionError("error connecting to {}:{}: {}".format(hostname, port, err))

        if result == 0:
            self.logger.debug("{} port {} is reachable".format(hostname, port))
            return True
        else:
            self.logger.debug("{} port {} is not reachable".format(hostname, port))
            raise NodeConnectionFailed("node {}:{} unreachable".format(hostname, port))

    @property
    def cb_parameters(self):
        if self.ssl:
            return "?ssl=no_verify&config_total_timeout=15&config_node_timeout=10&network=" + self.cb_network
        else:
            return "?config_total_timeout=15&config_node_timeout=10&network=" + self.cb_network

    @property
    def cb_connect_string(self):
        return self.cb_prefix + self.rally_host_name + self.cb_parameters

    @property
    def cb_network(self):
        if self.use_external_network:
            return 'external'
        else:
            return 'default'

    @property
    def admin_hostname(self):
        if self.node_cycle:
            return self.prefix + next(self.node_cycle) + ":" + self.admin_port
        else:
            return self.prefix + self.rally_cluster_node + ":" + self.admin_port

    @property
    def cb_sw_version(self):
        return self.sw_version

    @property
    def get_memory_quota(self):
        return self.memory_quota

    @property
    def is_node_api(self):
        return self.node_api_accessible

    def node_hostnames(self):
        for node in self.all_hosts:
            yield self.prefix + node + ":" + self.node_port

    def admin_api_get(self, endpoint):
        api_url = self.admin_hostname + endpoint
        self.logger.debug("admin_api_get connecting to {}".format(api_url))
        response = self.session.get(api_url, auth=(self.username, self.password), verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception as err:
            message = api_url + ": " + str(err)
            raise AdminApiError(message)

        response_json = json.loads(response.text)
        return response_json

    def node_api_get(self, endpoint):
        for node_name in list(self.node_hostnames()):
            api_url = node_name + endpoint
            self.logger.debug("node_api_get connecting to {}".format(api_url))
            response = self.session.get(api_url, auth=(self.username, self.password), verify=False, timeout=15)

            try:
                self.check_status_code(response.status_code)
            except NotFoundError:
                continue
            except Exception as err:
                message = api_url + ": " + str(err)
                raise NodeApiError(message)

            response_json = json.loads(response.text)
            yield response_json

    @retry(retry_count=10, allow_list=(TransientError, ClusterKVServiceError, ClusterHealthCheckError, NodeUnreachable, DNSLookupTimeout,
                                       ClusterInitError, NodeConnectionTimeout, NodeConnectionError, NodeConnectionFailed))
    def init_cluster(self):
        try:
            if not self.quick_connect:
                self.is_reachable()
        except Exception as err:
            raise ClusterInitError("cluster not reachable at {}: {}".format(self.rally_host_name, err))

        results = self.admin_api_get('/pools/default')

        if 'nodes' not in results:
            self.logger.error("init_cluster: invalid response from cluster.")
            raise ClusterInitError("Can not get node list from {}.".format(self.rally_host_name))

        for i in range(len(results['nodes'])):
            record = {}

            if 'alternateAddresses' in results['nodes'][i]:
                ext_host_name = results['nodes'][i]['alternateAddresses']['external']['hostname']
                record['external_name'] = ext_host_name
                record['external_ports'] = results['nodes'][i]['alternateAddresses']['external']['ports']
                self.external_network_present = True
                self.logger.info("Added external node {}".format(ext_host_name))

            host_name = results['nodes'][i]['configuredHostname']
            host_name = host_name.split(':')[0]

            record['host_name'] = host_name
            record['version'] = results['nodes'][i]['version']
            record['ostype'] = results['nodes'][i]['os']
            record['services'] = ','.join(results['nodes'][i]['services'])

            self.node_list.append(record)
            self.logger.info("Added node {}".format(host_name))

        self.cluster_info = results
        self.memory_quota = results['memoryQuota']
        self.sw_version = self.node_list[0]['version']

        if not self.quick_connect:
            try:
                self.cluster_health_check(restrict=False)
            except ClusterQueryServiceError:
                self.node_api_accessible = False
            except ClusterViewServiceError:
                pass
            except ClusterKVServiceError:
                raise ClusterInitError("KV service unhealthy")
            except ClusterHealthCheckError as err:
                raise ClusterInitError("cluster health check error: {}".format(err))
            except Exception:
                raise

        if self.use_external_network:
            self.all_hosts = list(self.node_list[i]['external_name'] for i, item in enumerate(self.node_list))
        else:
            self.all_hosts = list(self.node_list[i]['host_name'] for i, item in enumerate(self.node_list))

        self.node_cycle = cycle(self.all_hosts)
        self.logger.info("connected to cluster version {}".format(self.sw_version))
        return True

    def print_host_map(self):
        ext_host_name = None
        ext_port_list = None
        i = 0

        if self.rally_dns_domain:
            print("Name %s is a domain with SRV records:" % self.rally_host_name)
            for record in self.srv_host_list:
                print(" => %s (%s)" % (record['hostname'], record['address']))

        # if self.cluster_id:
        #     print("Capella cluster ID: {}".format(self.cluster_id))

        print("Cluster Host List:")
        for record in self.node_list:
            i += 1
            if 'external_name' in record:
                ext_host_name = record['external_name']
            if 'external_ports' in record:
                ext_port_list = record['external_ports']
            host_name = record['host_name']
            version = record['version']
            ostype = record['ostype']
            services = record['services']
            print(" [%02d] %s" % (i, host_name), end=' ')
            if ext_host_name:
                print("[external]> %s" % ext_host_name, end=' ')
            if ext_port_list:
                for key in ext_port_list:
                    print("%s:%s" % (key, ext_port_list[key]), end=' ')
            print("[Services] %s [version] %s [platform] %s" % (services, version, ostype))

    @retry(retry_count=10, allow_list=(CouchbaseTransientException, ProtocolException, ClusterHealthCheckError, TimeoutException, ClusterKVServiceError))
    def cluster_health_check(self, output=False, restrict=True):
        cb_auth = PasswordAuthenticator(self.username, self.password)
        cb_timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=60), kv_timeout=timedelta(seconds=60))

        try:
            cluster = Cluster(self.cb_connect_string, authenticator=cb_auth, lockmode=LOCKMODE_NONE, timeout_options=cb_timeouts)
            result = cluster.ping()
        except SystemError as err:
            if isinstance(err.__cause__, HTTPException) and err.__cause__.rc == 1049:
                self.use_external_network = not self.use_external_network
                raise ClusterHealthCheckError("HTTP unknown host: {}".format(err.__cause__))
            else:
                raise ClusterHealthCheckError("HTTP error: {}".format(err))
        except Exception as err:
            raise ClusterHealthCheckError("cluster health check error: {}".format(err))

        for endpoint, reports in result.endpoints.items():
            for report in reports:
                if restrict and endpoint.value != 'kv':
                    continue
                report_string = " {0}: {1} took {2} {3}".format(
                    endpoint.value,
                    report.remote,
                    report.latency,
                    report.state)
                if output:
                    print(report_string)
                    continue
                if not report.state == PingState.OK:
                    if endpoint.value == 'kv':
                        raise ClusterKVServiceError("{} KV service not ok".format(self.cb_connect_string))
                    elif endpoint.value == 'n1ql':
                        raise ClusterQueryServiceError("{} query service not ok".format(self.cb_connect_string))
                    elif endpoint.value == 'views':
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
                        report.state)
                    print(report_string)

        cluster.disconnect()
