##
##

import logging
from datetime import timedelta
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterTimeoutOptions, QueryOptions, ClusterOptions, LockMode, TLSVerifyMode
from couchbase.cluster import Cluster
from couchbase.diagnostics import ServiceType, PingState
from couchbase.exceptions import HTTPException
from acouchbase.cluster import AsyncCluster

logger = logging.getLogger(__name__)


class cbpool(object):

    def __init__(self, count: int, hostname: str, username: str, password: str, ssl=False, sync=True):
        self.pool_count = count
        self.sync = sync
        self.ssl = ssl
        self.username = username
        self.password = password
        self.rally_host_name = hostname
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

    @property
    def cb_connect_string(self):
        return self.cb_prefix + self.rally_host_name

    def connect(self):
        logger.debug(f"connect: connect string {self.cb_connect_string}")

        async def connect_a():

            cluster = AsyncCluster(self.cb_connect_string, ClusterOptions(self.auth,
                                                                          timeout_options=self.timeouts,
                                                                          lockmode=LockMode.WAIT,
                                                                          tls_verify=TLSVerifyMode.NO_VERIFY))
            result = await cluster.on_connect()
            return cluster

        def connect():
            cluster = Cluster(self.cb_connect_string, ClusterOptions(self.auth,
                                                                     timeout_options=self.timeouts,
                                                                     lockmode=LockMode.WAIT,
                                                                     tls_verify=TLSVerifyMode.NO_VERIFY))
            return cluster
