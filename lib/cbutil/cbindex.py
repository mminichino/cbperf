##
##

from .sessionmgr import cb_session
from .exceptions import *
import logging
import json


class cb_index(cb_session):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    def index_stats(self, bucket):
        index_data = {}
        endpoint = '/api/v1/stats/' + bucket
        for response_json in list(self.node_api_get(endpoint)):

            for key in response_json:
                index_name = key.split(':')[-1]
                index_object = key.split(':')[-2]
                if index_object not in index_data:
                    index_data[index_object] = {}
                if index_name not in index_data[index_object]:
                    index_data[index_object][index_name] = response_json[key]

        return index_data
