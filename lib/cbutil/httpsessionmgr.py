##
##

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
import logging
import base64
import os
from requests.auth import AuthBase
from .httpexceptions import NotAuthorized, HTTPForbidden, HTTPNotImplemented, RequestValidationError, InternalServerError


class basic_auth(AuthBase):

    def __init__(self, username, password):
        self.username = username
        self.password = password

    def __call__(self, r):
        auth_hash = f"{self.username}:{self.password}"
        auth_bytes = auth_hash.encode('ascii')
        auth_encoded = base64.b64encode(auth_bytes)
        request_headers = {
            "Authorization": f"Basic {auth_encoded.decode('ascii')}",
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

        if "HTTP_DEBUG_LEVEL" in os.environ:
            import http.client as http_client
            http_client.HTTPConnection.debuglevel = 1
            logging.basicConfig()
            self.debug_level = int(os.environ['HTTP_DEBUG_LEVEL'])
            requests_log = logging.getLogger("requests.packages.urllib3")
            if self.debug_level == 0:
                self.logger.setLevel(logging.DEBUG)
                requests_log.setLevel(logging.DEBUG)
            elif self.debug_level == 1:
                self.logger.setLevel(logging.INFO)
                requests_log.setLevel(logging.INFO)
            elif self.debug_level == 2:
                self.logger.setLevel(logging.ERROR)
                requests_log.setLevel(logging.ERROR)
            else:
                self.logger.setLevel(logging.CRITICAL)
                requests_log.setLevel(logging.CRITICAL)
            requests_log.propagate = True

    def check_status_code(self, code):
        self.logger.debug("API status code {}".format(code))
        if code == 200 or code == 201:
            return True
        elif code == 401:
            raise NotAuthorized("API: Unauthorized")
        elif code == 403:
            raise HTTPForbidden("API: Forbidden: Insufficient privileges")
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
        response = self.session.get(self.url_prefix + endpoint, auth=basic_auth(self.username, self.password), verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        try:
            response_data = json.loads(response.text)
        except ValueError:
            response_data = response.text
        return response_data

    def api_post(self, endpoint, body):
        response = self.session.post(self.url_prefix + endpoint, auth=basic_auth(self.username, self.password), json=body, verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        try:
            response_data = json.loads(response.text)
        except ValueError:
            response_data = response.text
        return response_data

    def api_put(self, endpoint, body):
        response = self.session.put(self.url_prefix + endpoint, auth=basic_auth(self.username, self.password), json=body, verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        try:
            response_data = json.loads(response.text)
        except ValueError:
            response_data = response.text
        return response_data

    def api_delete(self, endpoint):
        response = self.session.delete(self.url_prefix + endpoint, auth=basic_auth(self.username, self.password), verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        try:
            response_data = json.loads(response.text)
        except ValueError:
            response_data = response.text
        return response_data
