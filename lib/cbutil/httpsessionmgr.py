##
##

import requests
from requests.adapters import HTTPAdapter, Retry
import json
import logging
import base64
import os
import datetime
import hmac
import hashlib
import warnings
from urllib.parse import urlparse
from requests.auth import AuthBase
from .httpexceptions import NotAuthorized, HTTPForbidden, HTTPNotImplemented, RequestValidationError, InternalServerError, PaginationDataNotFound, SyncGatewayOperationException


class capella_auth(AuthBase):

    def __init__(self):
        if 'CBC_ACCESS_KEY' in os.environ:
            self.capella_key = os.environ['CBC_ACCESS_KEY']
        else:
            raise Exception("Please set CBC_ACCESS_KEY for Capella API access")

        if 'CBC_SECRET_KEY' in os.environ:
            self.capella_secret = os.environ['CBC_SECRET_KEY']
        else:
            raise Exception("Please set CBC_SECRET_KEY for Capella API access")

    def __call__(self, r):
        ep_path = urlparse(r.url).path
        ep_params = urlparse(r.url).query
        if len(ep_params) > 0:
            cbc_api_endpoint = ep_path + f"?{ep_params}"
        else:
            cbc_api_endpoint = ep_path
        cbc_api_method = r.method
        cbc_api_now = int(datetime.datetime.now().timestamp() * 1000)
        cbc_api_message = cbc_api_method + '\n' + cbc_api_endpoint + '\n' + str(cbc_api_now)
        cbc_api_signature = base64.b64encode(hmac.new(bytes(self.capella_secret, 'utf-8'),
                                                      bytes(cbc_api_message, 'utf-8'),
                                                      digestmod=hashlib.sha256).digest())
        cbc_api_request_headers = {
            'Authorization': 'Bearer ' + self.capella_key + ':' + cbc_api_signature.decode(),
            'Couchbase-Timestamp': str(cbc_api_now)
        }
        r.headers.update(cbc_api_request_headers)
        return r


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
    AUTH_BASIC = 0
    AUTH_CAPELLA = 1

    def __init__(self, username=None, password=None, auth_type=0):
        warnings.filterwarnings("ignore")
        self.username = username
        self.password = password
        self.logger = logging.getLogger(self.__class__.__name__)
        self.url_prefix = "http://127.0.0.1"
        self.session = requests.Session()
        retries = Retry(total=60,
                        backoff_factor=0.2)
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self._response = None
        if auth_type == 0:
            self.auth_class = basic_auth(self.username, self.password)
        else:
            self.auth_class = capella_auth()

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
        elif code == 415:
            raise RequestValidationError("API: invalid body contents")
        elif code == 422:
            raise RequestValidationError("API: Request Validation Error")
        elif code == 500:
            raise InternalServerError("API: Server Error")
        elif code == 503:
            raise SyncGatewayOperationException("API: Operation error code")
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

    def dump_json(self, indent=2):
        return json.dumps(self.json(), indent=indent)

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

    def capella_pagination(self, response_json):
        if "cursor" in response_json:
            if "pages" in response_json["cursor"]:
                if "items" in response_json["data"]:
                    data = response_json["data"]["items"]
                else:
                    data = response_json["data"]
                if "next" in response_json["cursor"]["pages"]:
                    next_page = response_json["cursor"]["pages"]["next"]
                    per_page = response_json["cursor"]["pages"]["perPage"]
                    return data, next_page, per_page
                else:
                    return data, None, None
        else:
            raise PaginationDataNotFound("pagination values not found")

    def api_get(self, endpoint, items=None):
        if items is None:
            items = []
        response = self.session.get(self.url_prefix + endpoint, auth=self.auth_class, verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        try:
            response_json = json.loads(response.text)
            data, next_page, per_page = self.capella_pagination(response_json)
            items.extend(data)
            if next_page:
                ep_path = urlparse(endpoint).path
                self.api_get(f"{ep_path}?page={next_page}&perPage={per_page}", items)
            response_text = json.dumps(items)
        except (PaginationDataNotFound, json.decoder.JSONDecodeError):
            response_text = response.text

        self._response = response_text
        return self

    def api_post(self, endpoint, body):
        response = self.session.post(self.url_prefix + endpoint, auth=self.auth_class, json=body, verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        self._response = response.text
        return self

    def api_put(self, endpoint, body):
        response = self.session.put(self.url_prefix + endpoint, auth=self.auth_class, json=body, verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        self._response = response.text
        return self

    def api_put_data(self, endpoint, body, content_type):
        headers = {'Content-Type': content_type}

        response = self.session.put(self.url_prefix + endpoint, auth=self.auth_class, data=body, verify=False, timeout=15, headers=headers)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        self._response = response.text
        return self

    def api_delete(self, endpoint):
        response = self.session.delete(self.url_prefix + endpoint, auth=self.auth_class, verify=False, timeout=15)

        try:
            self.check_status_code(response.status_code)
        except Exception:
            raise

        self._response = response.text
        return self
