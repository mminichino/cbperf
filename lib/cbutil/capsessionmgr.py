##
##

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
import logging
from .capauth import capella_auth
from .capexceptions import *


class capella_session(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.capella_url = 'https://cloudapi.cloud.couchbase.com'
        self.session = requests.Session()
        retries = Retry(total=60,
                        backoff_factor=0.1,
                        status_forcelist=[500, 501, 503])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

    def check_status_code(self, code):
        self.logger.debug("Capella API call status code {}".format(code))
        if code == 200:
            return True
        elif code == 403:
            raise CapellaNotAuthorized("Capella API: Forbidden: Insufficient privileges")
        elif code == 422:
            raise CapellaRequestValidationError("Capella API: Request Validation Error")
        elif code == 500:
            raise CapellaInternalServerError("Capella API: Server Error")
        else:
            raise Exception("Unknown Capella API call status code {}".format(code))

    def api_get(self, endpoint):
        response = self.session.get(self.capella_url + endpoint, auth=capella_auth())

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        response_json = json.loads(response.text)
        return response_json

    def api_post(self, endpoint, body):
        response = self.session.post(self.capella_url + endpoint, auth=capella_auth(), json=body)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        response_json = json.loads(response.text)
        return response_json

