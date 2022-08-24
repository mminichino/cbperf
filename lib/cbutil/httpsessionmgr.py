##
##

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
import logging
import base64
from requests.auth import AuthBase
from .httpexceptions import *


class basic_auth(AuthBase):

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def __call__(self, r):
        auth_hash = bytes(f"{self.username}:{self.password}")
        request_headers = {
            "Authorization": f"Basic {base64.b64encode(auth_hash)}",
        }
        r.headers.update(request_headers)
        return r


class api_session(object):
    HTTP = 0
    HTTPS = 1

    def __init__(self, username=None, password=None):
        self.username = username
        self.password = password
        self.logger = logging.getLogger(self.__class__.__name__)
        self.url_prefix = "http://127.0.0.1"
        self.session = requests.Session()
        retries = Retry(total=60,
                        backoff_factor=0.1,
                        status_forcelist=[500, 501, 503])
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self._response = None

    def check_status_code(self, code):
        self.logger.debug("API status code {}".format(code))
        if code == 200:
            return True
        elif code == 403:
            raise NotAuthorized("API: Forbidden: Insufficient privileges")
        elif code == 404:
            raise HTTPNotImplemented("API: Not Found")
        elif code == 422:
            raise RequestValidationError("API: Request Validation Error")
        elif code == 500:
            raise InternalServerError("API: Server Error")
        else:
            raise Exception("Unknown API status code {}".format(code))

    def set_host(self, hostname, ssl=0, port=None):
        if ssl == api_session.HTTP:
            port_num = port if port else 80
            self.url_prefix = f"http://{hostname}:{port_num}"
        else:
            port_num = port if port else 443
            self.url_prefix = f"https://{hostname}:{port_num}"

    @property
    def response(self):
        return self._response

    def json(self):
        return json.loads(self._response)

    def http_get(self, endpoint, headers=None, verify=False):
        response = self.session.get(self.url_prefix + endpoint, headers=headers, verify=verify)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        self._response = response.text
        return self

    def http_post(self, endpoint, data=None, headers=None, verify=False):
        response = self.session.post(self.url_prefix + endpoint, data=data, headers=headers, verify=verify)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        self._response = response.text
        return self

    def api_get(self, endpoint):
        response = self.session.get(self.url_prefix + endpoint, auth=basic_auth(self.username, self.password))

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        response_json = json.loads(response.text)
        return response_json

    def api_post(self, endpoint, body):
        response = self.session.post(self.url_prefix + endpoint, auth=basic_auth(self.username, self.password), json=body)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        response_json = json.loads(response.text)
        return response_json

