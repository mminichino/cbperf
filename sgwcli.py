#!/usr/bin/env python3

import argparse
import sys
import signal
import json
from lib.cbutil.httpsessionmgr import api_session
from lib.cbutil.httpexceptions import HTTPForbidden, HTTPNotImplemented


def break_signal_handler(signum, frame):
    print("")
    print("Break received, aborting.")
    sys.exit(1)


class sg_database(api_session):

    def __init__(self, node, *args, port=4985, ssl=0, **kwargs):
        super().__init__(*args, **kwargs)
        self.hostname = node
        self.set_host(node, ssl, port)

    def create(self, bucket, name, replicas=0):
        data = {
            "bucket": bucket,
            "name": name,
            "num_index_replicas": replicas
        }
        try:
            response = self.api_put(f"/{bucket}/", data)
            print(f"Database created for bucket {bucket}.")
        except HTTPForbidden:
            print(f"Bucket {bucket} does not exist.")
            sys.exit(1)
        except Exception as err:
            print(f"Database create failed for bucket {bucket}: {err}")
            sys.exit(1)

    def delete(self, name):
        try:
            response = self.api_delete(f"/{name}/")
            print(f"Database {name} deleted.")
        except HTTPForbidden:
            print(f"Database {name} does not exist.")
            sys.exit(1)
        except Exception as err:
            print(f"Database delete failed for {name}: {err}")
            sys.exit(1)

    def list(self, name):
        try:
            response = self.api_get(f"/{name}/_config")
            print(f"Bucket:   {response['bucket']}")
            print(f"Name:     {response['name']}")
            print(f"Replicas: {response['num_index_replicas']}")
        except HTTPForbidden:
            print(f"Database {name} does not exist.")
            sys.exit(1)
        except Exception as err:
            print(f"Database list failed for {name}: {err}")
            sys.exit(1)

    def dump(self, name):
        try:
            response = self.api_get(f"/{name}/_all_docs")
            for item in response["rows"]:
                print(f"Key: {item['key']} Id: {item['id']}")
        except HTTPForbidden:
            print(f"Database {name} does not exist.")
            sys.exit(1)
        except Exception as err:
            print(f"Database list failed for {name}: {err}")
            sys.exit(1)


class sg_user(api_session):

    def __init__(self, node, *args, port=4985, ssl=0, **kwargs):
        super().__init__(*args, **kwargs)
        self.hostname = node
        self.set_host(node, ssl, port)

    def create(self, dbname, username, password):
        data = {
            "password": password,
            "admin_channels": ["*"],
            "disabled": False
        }
        try:
            response = self.api_put(f"/{dbname}/_user/{username}", data)
            print(f"User {username} created for database {dbname}.")
        except HTTPForbidden:
            print(f"Database {dbname} does not exist.")
            sys.exit(1)
        except Exception as err:
            print(f"User create failed for database {dbname}: {err}")
            sys.exit(1)

    def delete(self, name, username):
        try:
            response = self.api_delete(f"/{name}/_user/{username}")
            print(f"User {username} deleted from {name}.")
        except HTTPForbidden:
            print(f"Database {name} does not exist.")
            sys.exit(1)
        except HTTPNotImplemented:
            print(f"User {username} does not exist.")
            sys.exit(1)
        except Exception as err:
            print(f"Database delete failed for {name}: {err}")
            sys.exit(1)

    def list(self, name, username=None):
        try:
            if username:
                response = self.api_get(f"/{name}/_user/{username}")
                print(f"Name:           {response['name']}")
                print(f"Admin channels: {response['admin_channels']}")
                print(f"All channels:   {response['all_channels']}")
                print(f"Disabled:       {response['disabled']}")
            else:
                response = self.api_get(f"/{name}/_user/")
                for item in response:
                    print(item)
        except HTTPForbidden:
            print(f"Database {name} does not exist.")
            sys.exit(1)
        except HTTPNotImplemented:
            print(f"User {username} does not exist.")
            sys.exit(1)
        except Exception as err:
            print(f"Database list failed for {name}: {err}")
            sys.exit(1)


def main():
    signal.signal(signal.SIGINT, break_signal_handler)

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument('-u', '--user', action='store', help="User Name", default="Administrator")
    parent_parser.add_argument('-p', '--password', action='store', help="User Password", default="password")
    parent_parser.add_argument('-h', '--host', action='store', help="Sync Gateway Hostname", default="localhost")
    parent_parser.add_argument('--help', action='help', default=argparse.SUPPRESS, help='Show help message')
    db_parser = argparse.ArgumentParser(add_help=False)
    db_parser.add_argument('-b', '--bucket', action='store', help='Bucket name')
    db_parser.add_argument('-n', '--name', action='store', help='Database name')
    db_parser.add_argument('-r', '--replicas', action='store', help='Replica count', type=int, default=0)
    user_parser = argparse.ArgumentParser(add_help=False)
    user_parser.add_argument('-n', '--name', action='store', help='Database name')
    user_parser.add_argument('--dbuser', action='store', help='Database user')
    user_parser.add_argument('--dbpass', action='store',  help='Database user password')
    main_parser = argparse.ArgumentParser(add_help=False)
    main_parser.add_argument('-h', '--help', action='help', default=argparse.SUPPRESS, help='Show help message')
    subparser = main_parser.add_subparsers(dest='command')
    db_mode = subparser.add_parser('database', help="Database Operations", parents=[parent_parser, db_parser], add_help=False)
    db_sub_mode = db_mode.add_subparsers(dest='db_command')
    db_sub_mode.add_parser('create', help="Create Database", parents=[parent_parser, db_parser], add_help=False)
    db_sub_mode.add_parser('delete', help="Delete Database", parents=[parent_parser, db_parser], add_help=False)
    db_sub_mode.add_parser('list', help="List Databases", parents=[parent_parser, db_parser], add_help=False)
    db_sub_mode.add_parser('dump', help="Dump Databases", parents=[parent_parser, db_parser], add_help=False)
    user_mode = subparser.add_parser('user', help="User Operations", parents=[parent_parser, user_parser], add_help=False)
    user_sub_mode = user_mode.add_subparsers(dest='user_command')
    user_sub_mode.add_parser('create', help="Add User", parents=[parent_parser, user_parser], add_help=False)
    user_sub_mode.add_parser('delete', help="Delete User", parents=[parent_parser, user_parser], add_help=False)
    user_sub_mode.add_parser('list', help="List Users", parents=[parent_parser, user_parser], add_help=False)
    parameters = main_parser.parse_args()

    if parameters.command == 'database':
        sgdb = sg_database(parameters.host, parameters.user, parameters.password)
        if parameters.db_command == "create":
            if not parameters.name:
                parameters.name = parameters.bucket
            sgdb.create(parameters.bucket, parameters.name, parameters.replicas)
        elif parameters.db_command == "delete":
            sgdb.delete(parameters.name)
        elif parameters.db_command == "list":
            sgdb.list(parameters.name)
        elif parameters.db_command == "dump":
            sgdb.dump(parameters.name)
    elif parameters.command == 'user':
        sguser = sg_user(parameters.host, parameters.user, parameters.password)
        if parameters.user_command == "create":
            sguser.create(parameters.name, parameters.dbuser, parameters.dbpass)
        elif parameters.user_command == "delete":
            sguser.delete(parameters.name, parameters.dbuser)
        elif parameters.user_command == "list":
            sguser.list(parameters.name, parameters.dbuser)


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
