##
##

from .capsessionmgr import capella_session
from .capexceptions import *
import logging


class capella_api(capella_session):

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        self._cluster_id = None
        self._cluster_name = None

        if 'CAPELLA_HOST_NAME' in os.environ:
            self._cluster_name = os.environ['CAPELLA_HOST_NAME']
        else:
            raise CapellaMissingClusterName("Please set CAPELLA_HOST_NAME for Capella API access")

    @property
    def cluster_id(self):
        return self._cluster_id

    def connect(self):
        try:
            self._cluster_id = self.get_cluster_id(self._cluster_name)
        except Exception as err:
            raise CapellaConnectException(f"can not connect to Capella cluster at {self._cluster_name}: {err}")

    def get_cluster_id(self, name):
        cluster_id = None

        results = self.api_get('/v3/clusters')

        if 'data' in results:
            for item in results['data']['items']:
                if item['name'] == name:
                    cluster_id = item['id']

        if not cluster_id:
            raise CapellaClusterNotFound(f"Capella cluster {name} not found")

        return cluster_id

    def capella_create_bucket(self, bucket, quota):
        raise CapellaNotImplemented(f"please create bucket {bucket} in the Capella UI to continue")

    def capella_delete_bucket(self, bucket):
        raise CapellaNotImplemented(f"please delete bucket {bucket} in the Capella UI")
