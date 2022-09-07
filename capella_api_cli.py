#!/usr/bin/env -S python3

import argparse
import json
import re
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


class cluster_mgr(object):

    def __init__(self):
        self.api = api_session(auth_type=api_session.AUTH_CAPELLA)
        self.api.set_host("cloudapi.cloud.couchbase.com", api_session.HTTPS)

    def cluster_get(self, name):
        clusters = self.api.api_get("/v3/clusters").json()
        for item in clusters:
            if item["name"] == name:
                try:
                    cluster_info = self.api.api_get(f"/v3/clusters/{item['id']}")
                    print(cluster_info.dump_json())
                    break
                except HTTPNotImplemented:
                    print(f"Cluster details for {name} were not found.")


def main():
    warnings.filterwarnings("ignore")
    signal.signal(signal.SIGINT, break_signal_handler)
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', action='store')
    parser.add_argument('-g', action='store_true')
    parser.add_argument('-c', action='store_true')
    # main_parser = argparse.ArgumentParser(add_help=False)
    # main_parser.add_argument('-h', '--help', action='help', default=argparse.SUPPRESS, help='Show help message')
    command_parser = parser.add_subparsers(dest='command')
    cluster_parser = command_parser.add_parser('cluster', help="Cluster Operations", parents=[], add_help=False)
    cluster_command_parser = cluster_parser.add_subparsers(dest='cluster_command')
    cluster_command_get = cluster_command_parser.add_parser('get', help="Get Cluster Info", parents=[], add_help=False)
    cluster_command_get.add_argument('remainder', nargs=argparse.REMAINDER)
    args = parser.parse_args()

    api = api_session(auth_type=api_session.AUTH_CAPELLA)
    api.set_host("cloudapi.cloud.couchbase.com", api_session.HTTPS)

    endpoint = args.e

    try:
        if args.g:
            result = api.api_get(endpoint)
            print(result.dump_json())
        elif args.c:
            result = api.api_get(endpoint)
            print(len(result.json()))
        elif args.command == 'cluster':
            cm = cluster_mgr()
            if args.cluster_command == "get":
                cm.cluster_get(args.remainder[0])
    except HTTPNotImplemented:
        sys.exit(404)
    except Exception:
        raise


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
