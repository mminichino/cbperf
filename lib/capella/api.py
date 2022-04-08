##
##

from .session import capella_session
from .exceptions import *
import logging


class capella_api(capella_session):

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_cluster_id(self, name):
        cluster_id = None

        results = self.api_get('/v3/clusters')

        if 'data' in results:
            for item in results['data']['items']:
                if item['name'] == name:
                    cluster_id = item['id']

        if not cluster_id:
            raise ClusterNotFound("Capella cluster {} not found".format(name))

        return cluster_id

