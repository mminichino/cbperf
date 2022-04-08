##
##

import base64
import hmac
import hashlib
import datetime
from urllib.parse import urlparse
from requests.auth import AuthBase
from .exceptions import *


class capella_auth(AuthBase):

    def __init__(self):
        if 'CAPELLA_ACCESS_KEY_ID' in os.environ:
            self.capella_key = os.environ['CAPELLA_ACCESS_KEY_ID']
        else:
            raise MissingAuthKey("Please set CAPELLA_ACCESS_KEY_ID for Capella API access")

        if 'CAPELLA_SECRET_ACCESS_KEY' in os.environ:
            self.capella_secret = os.environ['CAPELLA_SECRET_ACCESS_KEY']
        else:
            raise MissingSecretKey("Please set CAPELLA_SECRET_ACCESS_KEY for Capella API access")

    def __call__(self, r):
        cbc_api_endpoint = urlparse(r.url).path
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
