#!/usr/bin/env -S python3

import argparse
import json
import sys
import signal
import warnings
from lib.cbutil.capsessionmgr import capella_session
from lib.cbutil.httpsessionmgr import api_session
from lib.cbutil.httpexceptions import HTTPForbidden, HTTPNotImplemented


def break_signal_handler(signum, frame):
    print("")
    print("Break received, aborting.")
    sys.exit(1)


def main():
    warnings.filterwarnings("ignore")
    signal.signal(signal.SIGINT, break_signal_handler)
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', action='store', required=True)
    parser.add_argument('-g', action='store_true')
    parser.add_argument('-c', action='store_true')
    args = parser.parse_args()

    api = api_session(auth_type=api_session.AUTH_CAPELLA)
    api.set_host("cloudapi.cloud.couchbase.com", api_session.HTTPS)

    endpoint = args.e

    if args.g:
        result = api.api_get(endpoint)
        print(result.dump_json())
    elif args.c:
        result = api.api_get(endpoint)
        print(len(result.json()))


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
