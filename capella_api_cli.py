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

    def service_sort(self, service_list):
        ref = ['data', 'index', 'query', 'search', 'eventing', 'analytics']
        result = []
        for item in ref:
            if item in service_list:
                result.append(item)
        return result

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

    def cluster_list(self):
        clusters = self.api.api_get("/v3/clusters").json()
        print("Type   "
              "Name                                     "
              "ID                                   "
              "Project                              "
              "Cloud Region               "
              "#   "
              "Stor  "
              "Ver   "
              "MDS Services                                    "
              "Nodes")
        print("====== "
              "======================================== "
              "==================================== "
              "==================================== "
              "===== "
              "==================== "
              "=== "
              "===== "
              "===== "
              "=== "
              "=========================================== "
              "=======================================")
        for item in clusters:
            print(f"{item['environment'].ljust(6)} {item['name'].ljust(40)} {item['id']} {item['projectId']} ", end='')
            try:
                node_total = 0
                storage_total = 0
                type_list = []
                service_list = []
                last_list = []
                mds = "N"
                cluster_info = self.api.api_get(f"/v3/clusters/{item['id']}").json()
                for server_group in cluster_info['servers']:
                    node_total += server_group['size']
                    storage_total += (server_group['storage']['size'] * server_group['size'])
                    type_list.append(server_group['compute'])
                    current_list = server_group['services']
                    service_list.extend(current_list)
                    current_list.sort()
                    if len(last_list) > 0:
                        if last_list != current_list:
                            mds = "Y"
                    last_list = current_list
                service_list = self.service_sort([*set(service_list)])
                type_list = [*set(type_list)]
                print(f"{cluster_info['place']['provider'].ljust(5)} "
                      f"{cluster_info['place']['region'].ljust(20)} "
                      f"{str(node_total).ljust(3)} {str(storage_total).ljust(5)} "
                      f"{cluster_info['version']['components']['cbServerVersion']} "
                      f"{mds.ljust(3)} "
                      f"{','.join(service_list).ljust(43)} "
                      f"{','.join(type_list)}")
            except HTTPNotImplemented:
                print("")
                continue


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
    cluster_command_list = cluster_command_parser.add_parser('list', help="Get Cluster List", parents=[], add_help=False)
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
            elif args.cluster_command == "list":
                cm.cluster_list()
    except HTTPNotImplemented:
        sys.exit(404)
    except Exception:
        raise


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
