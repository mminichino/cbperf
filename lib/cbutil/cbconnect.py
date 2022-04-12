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

    def index_stats(self, bucket):
        index_data = {}
        endpoint = '/api/v1/stats/' + bucket
        for response_json in list(self.node_api_get(endpoint)):

            for key in response_json:
                index_name = key.split(':')[-1]
                if index_name not in index_data:
                    index_data[index_name] = {}
                for attribute in response_json[key]:
                    if attribute not in index_data[index_name]:
                        index_data[index_name][attribute] = response_json[key][attribute]
                    else:
                        index_data[index_name][attribute] += response_json[key][attribute]

        return index_data
