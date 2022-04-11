##
##

from .sessionmgr import cb_session
from .exceptions import *
import logging
import json


class cb_connect(cb_session):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_cluster_info(self):
        results = self.admin_api_get('/pools/default')

        return results
