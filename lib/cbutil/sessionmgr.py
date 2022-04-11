##
##

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
import logging
import socket
import dns.resolver
from itertools import cycle
from .exceptions import *


class cb_session(object):

    def __init__(self, hostname: str, username: str, password: str, ssl=True, external=False):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.rally_host_name = hostname
        self.rally_cluster_node = self.rally_host_name
        self.node_list = []
        self.srv_host_list = []
        self.rally_dns_domain = False
        self.node_cycle = None
        self.cluster_info = None
        self.sw_version = None
        self.username = username
        self.password = password
        self.ssl = ssl
        self.use_external_network = external
        self.external_network_present = False
        self.node_api_accessible = True

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
        self.logger.debug("Capella API call status code {}".format(code))
        if code == 200:
            return True
        elif code == 401:
            raise NotAuthorized("Capella API: Forbidden: Insufficient privileges")
        elif code == 403:
            raise ForbiddenError("Capella API: Request Validation Error")
        elif code == 404:
            raise NotFoundError("Capella API: Server Error")
        else:
            raise Exception("Unknown Capella API call status code {}".format(code))

    def is_reachable(self):
        resolver = dns.resolver.Resolver()

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
        except Exception:
            raise

        if not self.check_node_connectivity(self.rally_cluster_node, self.admin_port):
            raise NodeUnreachable("can not connect to node {}".format(self.rally_cluster_node))

        self.logger.info("initial connect node name: {}".format(self.rally_cluster_node))
        self.logger.debug("is_reachable: rally_host_name: {}".format(self.rally_host_name))
        self.logger.debug("is_reachable: rally_cluster_node: {}".format(self.rally_cluster_node))
        return True

    def check_node_connectivity(self, hostname, port):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex((hostname, int(port)))
            sock.close()
        except Exception:
            self.logger.debug("{} port {} lookup failure".format(hostname, port))
            return False
        if result == 0:
            self.logger.debug("{} port {} is reachable".format(hostname, port))
            return True
        else:
            self.logger.debug("{} port {} is not reachable".format(hostname, port))
            return False

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

    def admin_api_get(self, endpoint):
        api_url = self.admin_hostname + endpoint
        self.logger.debug("admin_api_get connecting to {}".format(api_url))
        response = self.session.get(api_url, auth=(self.username, self.password), verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        response_json = json.loads(response.text)
        return response_json

    def init_cluster(self):
        try:
            self.is_reachable()
        except Exception as e:
            raise ClusterInitError("cluster not reachable at {}: {}".format(self.rally_host_name, e))

        results = self.admin_api_get('/pools/default')

        if 'nodes' not in results:
            self.logger.error("init_cluster: invalid response from cluster.")
            raise ClusterInitError("Can not get node list from {}.".format(self.rally_host_name))

        for i in range(len(results['nodes'])):
            record = {}
            ext_host_name = None
            host_name = None

            if 'alternateAddresses' in results['nodes'][i]:
                ext_host_name = results['nodes'][i]['alternateAddresses']['external']['hostname']
                if self.check_node_connectivity(ext_host_name, self.admin_port):
                    self.logger.info("external name {} is reachable".format(ext_host_name))
                    self.external_network_present = True
                    record['external_name'] = ext_host_name
                    record['external_ports'] = results['nodes'][i]['alternateAddresses']['external']['ports']
                    self.logger.info("Added external node {}".format(ext_host_name))
                    # if self.check_node_connectivity(ext_host_name, self.node_port):
                    #     self.logger.info("node API port accessible on {}".format(ext_host_name))
                    #     record['external_node_api'] = True
                    # else:
                    #     self.logger.info("node API port not accessible on {}".format(ext_host_name))
                    #     record['external_node_api'] = False

            host_name = results['nodes'][i]['configuredHostname']
            host_name = host_name.split(':')[0]

            if not self.check_node_connectivity(host_name, self.admin_port):
                self.logger.info("node name {} is not reachable".format(host_name))
                if self.external_network_present:
                    self.use_external_network = True
                else:
                    raise NodeUnreachable("can not connect to node {}".format(host_name))

            record['host_name'] = host_name
            record['version'] = results['nodes'][i]['version']
            record['ostype'] = results['nodes'][i]['os']
            record['services'] = ','.join(results['nodes'][i]['services'])
            self.node_list.append(record)
            self.logger.info("Added node {}".format(host_name))

        self.cluster_info = results
        self.sw_version = self.node_list[0]['version']
        self.logger.info("connected to cluster version {}".format(self.sw_version))
        return True
