#!/usr/bin/env -S python3

import argparse
import sys
import signal
import warnings
from datetime import datetime, timezone
from lib.cbutil.httpsessionmgr import api_session
from lib.cbutil.httpexceptions import HTTPNotImplemented


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
              "Project                    "
              "Cloud "
              "Region               "
              "#   "
              "Stor  "
              "Ver   "
              "MDS "
              "Age        "
              "CIDR            "
              "Services                                    "
              "Nodes")
        print("====== "
              "======================================== "
              "==================================== "
              "========================== "
              "===== "
              "==================== "
              "=== "
              "===== "
              "===== "
              "=== "
              "========== "
              "=============== "
              "=========================================== "
              "=======================================")
        for item in clusters:
            project_info = self.api.api_get(f"/v2/projects/{item['projectId']}").json()
            print(f"{item['environment'].ljust(6)} "
                  f"{item['name'].ljust(40)} "
                  f"{item['id']} "
                  f"{project_info['name'].ljust(26)} ", end='')
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
                created = cluster_info['createdAt']
                created = created[:-4]+created[-1]
                create_time = datetime.strptime(created, '%Y-%m-%dT%H:%M:%S.%f%z')
                now = datetime.now(timezone.utc)
                delta = now - create_time
                d_days = str(delta.days)
                d_hours = str(round(delta.seconds / 3600))
                print(f"{cluster_info['place']['provider'].ljust(5)} "
                      f"{cluster_info['place']['region'].ljust(20)} "
                      f"{str(node_total).ljust(3)} {str(storage_total).ljust(5)} "
                      f"{cluster_info['version']['components']['cbServerVersion']} "
                      f"{mds.ljust(3)} "
                      f"{d_days.rjust(5)}d {d_hours.rjust(2)}h "
                      f"{cluster_info['place']['CIDR'].ljust(16)}"
                      f"{','.join(service_list).ljust(43)} "
                      f"{','.join(type_list)}")
            except HTTPNotImplemented:
                print("")
                continue

    def get_projects(self):
        projects = self.api.api_get("/v2/projects").json()
        for project in projects:
            print(f"{project['id']} {project['name']}")


def main():
    warnings.filterwarnings("ignore")
    signal.signal(signal.SIGINT, break_signal_handler)
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', action='store')
    parser.add_argument('-g', action='store_true')
    parser.add_argument('-c', action='store_true')
    command_parser = parser.add_subparsers(dest='command')
    cluster_parser = command_parser.add_parser('cluster', help="Cluster Operations", parents=[], add_help=False)
    cluster_command_parser = cluster_parser.add_subparsers(dest='cluster_command')
    cluster_command_get = cluster_command_parser.add_parser('get', help="Get Cluster Info", parents=[], add_help=False)
    cluster_command_list = cluster_command_parser.add_parser('list', help="Get Cluster List", parents=[], add_help=False)
    cluster_command_get.add_argument('remainder', nargs=argparse.REMAINDER)
    project_parser = command_parser.add_parser('project', help="Project Operations", parents=[], add_help=False)
    project_command_parser = project_parser.add_subparsers(dest='project_command')
    project_command_list = project_command_parser.add_parser('list', help="Get Project List", parents=[], add_help=False)
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
        elif args.command == 'project':
            cm = cluster_mgr()
            if args.project_command == "list":
                cm.get_projects()
    except HTTPNotImplemented:
        sys.exit(404)
    except Exception:
        raise


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
