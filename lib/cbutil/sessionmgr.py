##
##

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
import socket
import dns.resolver
from itertools import cycle
from datetime import timedelta
from couchbase.exceptions import (HTTPException)
from couchbase.diagnostics import PingState
from couchbase.cluster import Cluster, QueryOptions, ClusterTimeoutOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.diagnostics import ServiceType
from .exceptions import *
from .capexceptions import *
from .retries import retry
from .capella import capella_api


class cb_session_cache(object):

    def __init__(self):
        self._node_list = []
        self._memory_quota = None
        self._cluster_info = None
        self._sw_version = None
        self._all_hosts = []
        self._node_api_accessible = True
        self._external_network_present = False
        self._use_external_network = False
        self._rally_cluster_node = None
        self._srv_host_list = []
        self._rally_dns_domain = False
        self._capella_target = False
        self._capella_session = None

    def extract(self, session):
        self._node_list = session.node_list
        self._memory_quota = session.memory_quota
        self._cluster_info = session.cluster_info
        self._sw_version = session.sw_version
        self._all_hosts = session.all_hosts
        self._node_api_accessible = session.node_api_accessible
        self._external_network_present = session.external_network_present
        self._use_external_network = session.use_external_network
        self._rally_cluster_node = session.rally_cluster_node
        self._srv_host_list = session.srv_host_list
        self._rally_dns_domain = session.rally_dns_domain
        self._capella_target = session.capella_target
        self._capella_session = session.capella_session

    def store_node_list(self, value):
        self._node_list = value

    def store_memory_quota(self, value):
        self._memory_quota = value

    def store_cluster_info(self, value):
        self._cluster_info = value

    def store_sw_version(self, value):
        self._sw_version = value

    def store_all_hosts(self, value):
        self._all_hosts = value

    def store_node_api_accessible(self, value):
        self._node_api_accessible = value

    def store_external_network_present(self, value):
        self._external_network_present = value

    def store_use_external_network(self, value):
        self._use_external_network = value

    def store_rally_cluster_node(self, value):
        self._rally_cluster_node = value

    def store_srv_host_list(self, value):
        self._srv_host_list = value

    def store_rally_dns_domain(self, value):
        self._rally_dns_domain = value

    @property
    def node_list(self):
        return self._node_list

    @property
    def memory_quota(self):
        return self._memory_quota

    @property
    def cluster_info(self):
        return self._cluster_info

    @property
    def sw_version(self):
        return self._sw_version

    @property
    def all_hosts(self):
        return self._all_hosts

    @property
    def node_api_accessible(self):
        return self._node_api_accessible

    @property
    def external_network_present(self):
        return self._external_network_present

    @property
    def use_external_network(self):
        return self._use_external_network

    @property
    def rally_cluster_node(self):
        return self._rally_cluster_node

    @property
    def srv_host_list(self):
        return self._srv_host_list

    @property
    def rally_dns_domain(self):
        return self._rally_dns_domain

    @property
    def capella_target(self):
        return self._capella_target

    @property
    def capella_session(self):
        return self._capella_session


class cb_session(object):

    def __init__(self, hostname: str, username: str, password: str, ssl=False, external=False, restore=None, cloud=True):
        self.rally_host_name = hostname
        self.rally_cluster_node = self.rally_host_name
        self.node_list = []
        self.srv_host_list = []
        self.all_hosts = []
        self.cluster_services = []
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
        self.restore_session = restore
        self.cloud_api = cloud
        self._session_cache = cb_session_cache()
        self.capella_target = False
        self.capella_session = None

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

        self.session = requests.Session()
        retries = Retry(total=60,
                        backoff_factor=0.1,
                        status_forcelist=[500, 501, 503])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

        self.init_cluster()

    def check_status_code(self, code, endpoint):
        if code == 200:
            return True
        elif code == 401:
            raise NotAuthorized(f"{endpoint}: Unauthorized: Insufficient privileges")
        elif code == 403:
            raise ForbiddenError(f"{endpoint}: Forbidden")
        elif code == 404:
            raise NotFoundError(f"{endpoint}: Not Found")
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
            self.rally_cluster_node = self.srv_host_list[0]['hostname']
            self.rally_dns_domain = True
        except dns.resolver.NXDOMAIN:
            pass
        except dns.exception.Timeout:
            raise DNSLookupTimeout("{} lookup timeout".format(self.srv_prefix + self.rally_host_name))
        except Exception:
            raise

        try:
            self.check_node_connectivity(self.rally_cluster_node, self.admin_port)
        except (NodeConnectionTimeout, NodeConnectionError, NodeConnectionFailed) as err:
            raise NodeUnreachable("can not connect to node {}: {}".format(self.rally_cluster_node, err))

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
            return True
        else:
            raise NodeConnectionFailed("node {}:{} unreachable".format(hostname, port))

    @property
    def cb_parameters(self):
        if self.ssl:
            return "?ssl=no_verify&network=" + self.cb_network
        else:
            return "?network=" + self.cb_network

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
        response = self.session.get(api_url, auth=(self.username, self.password), verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code, endpoint)
        except Exception as err:
            message = api_url + ": " + str(err)
            raise AdminApiError(message)

        response_json = json.loads(response.text)
        return response_json

    def node_api_get(self, endpoint):
        for node_name in list(self.node_hostnames()):
            api_url = node_name + endpoint
            response = self.session.get(api_url, auth=(self.username, self.password), verify=False, timeout=15)

            try:
                self.check_status_code(response.status_code, endpoint)
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
        if self.restore_session:
            try:
                self.node_list = self.restore_session.node_list
                self.memory_quota = self.restore_session.memory_quota
                self.cluster_info = self.restore_session.cluster_info
                self.sw_version = self.restore_session.sw_version
                self.all_hosts = self.restore_session.all_hosts
                self.node_api_accessible = self.restore_session.node_api_accessible
                self.external_network_present = self.restore_session.external_network_present
                self.use_external_network = self.restore_session.use_external_network
                self.rally_cluster_node = self.restore_session.rally_cluster_node
                self.srv_host_list = self.restore_session.srv_host_list
                self.rally_dns_domain = self.restore_session.rally_dns_domain
                self.capella_target = self.restore_session.capella_target
                self.capella_session = self.restore_session.capella_session
                self.node_cycle = cycle(self.all_hosts)
                return True
            except Exception as err:
                raise ClusterInitError(f"can not read restore cache: {err}")

        try:
            self.is_reachable()
        except Exception as err:
            raise ClusterInitError("cluster not reachable at {}: {}".format(self.rally_host_name, err))

        domain_name_check = '.'.join(self.rally_host_name.split('.')[-3:])
        if domain_name_check == 'cloud.couchbase.com' and self.cloud_api:
            self.capella_target = True
            try:
                self.capella_session = capella_api()
                self.capella_session.connect()
            except Exception as err:
                raise ClusterInitError(f"{err}")

        results = self.admin_api_get('/pools/default')

        if 'nodes' not in results:
            raise ClusterInitError("Can not get node list from {}.".format(self.rally_host_name))

        for i in range(len(results['nodes'])):
            record = {}

            if 'alternateAddresses' in results['nodes'][i]:
                ext_host_name = results['nodes'][i]['alternateAddresses']['external']['hostname']
                record['external_name'] = ext_host_name
                record['external_ports'] = results['nodes'][i]['alternateAddresses']['external']['ports']
                self.external_network_present = True

            host_name = results['nodes'][i]['configuredHostname']
            host_name = host_name.split(':')[0]

            record['host_name'] = host_name
            record['version'] = results['nodes'][i]['version']
            record['ostype'] = results['nodes'][i]['os']
            record['services'] = ','.join(results['nodes'][i]['services'])
            self.cluster_services = list(results['nodes'][i]['services'])

            self.node_list.append(record)

        self.cluster_info = results
        self.memory_quota = results['memoryQuota']
        self.sw_version = self.node_list[0]['version']

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
        self._session_cache.extract(self)
        return True

    @property
    def session_cache(self):
        return self._session_cache

    def print_host_map(self):
        ext_host_name = None
        ext_port_list = None
        i = 0

        if self.rally_dns_domain:
            print("Name %s is a domain with SRV records:" % self.rally_host_name)
            for record in self.srv_host_list:
                print(" => %s (%s)" % (record['hostname'], record['address']))

        if self.capella_session:
            print(f"Capella cluster ID: {self.capella_session.cluster_id}")

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

    @retry(retry_count=10)
    def cluster_health_check(self, output=False, restrict=True, noraise=False, extended=False):
        cb_auth = PasswordAuthenticator(self.username, self.password)
        cb_timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=60), kv_timeout=timedelta(seconds=60))

        try:
            cluster = Cluster(self.cb_connect_string, authenticator=cb_auth, timeout_options=cb_timeouts)
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
