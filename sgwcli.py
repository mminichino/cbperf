#!/usr/bin/env python3

import argparse
import sys
import signal
import json
from lib.cbutil.httpsessionmgr import api_session
from lib.cbutil.httpexceptions import HTTPForbidden, HTTPNotImplemented, SyncGatewayOperationException
from lib.cbutil.cbsync import cb_connect_s
from lib.cbutil.retries import retry


def break_signal_handler(signum, frame):
    print("")
    print("Break received, aborting.")
    sys.exit(1)


class cb_interface(object):

    def __init__(self, hostname, username, password, ssl=True):
        self.host = hostname
        self.username = username
        self.password = password
        self.ssl = ssl

    def get_values(self, field, keyspace):
        query = f"select distinct {field} from {keyspace} where {field} is not missing;"
        usernames = []

        try:
            db = cb_connect_s(self.host, self.username, self.password, ssl=self.ssl).init()
            results = db.cb_query(sql=query)
            for record in results:
                value = record[field]
                usernames.append(f"{field}@{value}")
            return usernames
        except Exception as err:
            print(f"Can not get the values for {field}: {err}")
            sys.exit(1)


class sg_database(api_session):

    def __init__(self, node, *args, port=4985, ssl=0, **kwargs):
        super().__init__(*args, **kwargs)
        self.hostname = node
        self.set_host(node, ssl, port)

    def create(self, bucket, name, replicas=0):
        data = {
            "import_docs": True,
            "enable_shared_bucket_access": True,
            "bucket": bucket,
            "name": name,
            "num_index_replicas": replicas
        }
        try:
            response = self.api_put(f"/{name}/", data)
            print(f"Database {name} created for bucket {bucket}.")
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

    def sync_fun(self, name, filename):
        with open(filename, "r") as file:
            data = file.read()
            file.close()
            try:
                response = self.api_put_data(f"/{name}/_config/sync", data, 'application/javascript')
                print(f"Sync function created for database {name}.")
            except HTTPForbidden:
                print(f"Database {name} does not exist.")
                sys.exit(1)
            except Exception as err:
                print(f"Sync function create failed for database {name}: {err}")
                sys.exit(1)

    def get_sync_fun(self, name):
        try:
            response = self.api_get(f"/{name}/_config/sync")
            print(response.response)
        except HTTPForbidden:
            print(f"Database {name} does not exist.")
            sys.exit(1)
        except Exception as err:
            print(f"Sync function get failed for database {name}: {err}")
            sys.exit(1)

    def resync(self, name):
        try:
            response = self.api_post(f"/{name}/_offline", None)
            response = self.api_post(f"/{name}/_resync", None)
            print("Waiting for resync to complete")
            self.resync_wait(name)
            print("Resync complete")
        except HTTPForbidden:
            print(f"Database {name} does not exist.")
            sys.exit(1)
        except Exception as err:
            print(f"Resync failed for database {name}: {err}")
            sys.exit(1)

    @retry(factor=0.5, retry_count=20)
    def resync_wait(self, name):
        response = self.api_post(f"/{name}/_online", None)

    def list(self, name):
        try:
            response = self.api_get(f"/{name}/_config").json()
            print(f"Bucket:   {response['bucket']}")
            print(f"Name:     {response['name']}")
            print(f"Replicas: {response['num_index_replicas']}")
        except HTTPForbidden:
            print(f"Database {name} does not exist.")
            sys.exit(1)
        except Exception as err:
            print(f"Database list failed for {name}: {err}")
            sys.exit(1)

    @retry(factor=0.5, retry_count=20)
    def ready_wait(self, name):
        response = self.api_get(f"/{name}/_config").json()

    def dump(self, name):
        try:
            response = self.api_get(f"/{name}/_all_docs").json()
            for item in response["rows"]:
                document = self.api_get(f"/{name}/_raw/{item['id']}").json()
                print(f"Key: {item['key']} Id: {item['id']} Channels: {document['_sync']['history']['channels']}")
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

    def create(self, dbname, username, password, channels=None):
        if channels is None:
            admin_channels = "*"
        else:
            admin_channels = channels
        data = {
            "password": password,
            "admin_channels": [admin_channels],
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
                response = self.api_get(f"/{name}/_user/{username}").json()
                print(f"Name:           {response['name']}")
                print(f"Admin channels: {response['admin_channels']}")
                print(f"All channels:   {response['all_channels']}")
                print(f"Roles:          {response['admin_roles']}")
                print(f"Disabled:       {response['disabled']}")
            else:
                response = self.api_get(f"/{name}/_user/").json()
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
    db_parser.add_argument('-f', '--function', action='store', help='Sync Function')
    db_parser.add_argument('-r', '--replicas', action='store', help='Replica count', type=int, default=0)
    db_parser.add_argument('-g', '--get', action='store_true', help='Get Sync Function')
    user_parser = argparse.ArgumentParser(add_help=False)
    user_parser.add_argument('-n', '--name', action='store', help='Database name')
    user_parser.add_argument('-U', '--sguser', action='store', help='SGW user name', default="sguser")
    user_parser.add_argument('-P', '--sgpass', action='store',  help='SGW user password', default="password")
    user_parser.add_argument('-d', '--dbhost', action='store', help='Couchbase hostname', default="localhost")
    user_parser.add_argument('-l', '--dblogin', action='store', help='Couchbase credentials', default="Administrator:password")
    user_parser.add_argument('-f', '--field', action='store', help='Document field')
    user_parser.add_argument('-k', '--keyspace', action='store', help='Keyspace')
    user_parser.add_argument('-a', '--all', action='store_true', help='List all users')
    main_parser = argparse.ArgumentParser(add_help=False)
    main_parser.add_argument('-h', '--help', action='help', default=argparse.SUPPRESS, help='Show help message')
    subparser = main_parser.add_subparsers(dest='command')
    db_mode = subparser.add_parser('database', help="Database Operations", parents=[parent_parser, db_parser], add_help=False)
    db_sub_mode = db_mode.add_subparsers(dest='db_command')
    db_sub_mode.add_parser('create', help="Create Database", parents=[parent_parser, db_parser], add_help=False)
    db_sub_mode.add_parser('delete', help="Delete Database", parents=[parent_parser, db_parser], add_help=False)
    db_sub_mode.add_parser('sync', help="Add Sync Function", parents=[parent_parser, db_parser], add_help=False)
    db_sub_mode.add_parser('resync', help="Sync Documents", parents=[parent_parser, db_parser], add_help=False)
    db_sub_mode.add_parser('list', help="List Databases", parents=[parent_parser, db_parser], add_help=False)
    db_sub_mode.add_parser('dump', help="Dump Databases", parents=[parent_parser, db_parser], add_help=False)
    db_sub_mode.add_parser('wait', help="Wait For Database Online", parents=[parent_parser, db_parser], add_help=False)
    user_mode = subparser.add_parser('user', help="User Operations", parents=[parent_parser, user_parser], add_help=False)
    user_sub_mode = user_mode.add_subparsers(dest='user_command')
    user_sub_mode.add_parser('create', help="Add User", parents=[parent_parser, user_parser], add_help=False)
    user_sub_mode.add_parser('delete', help="Delete User", parents=[parent_parser, user_parser], add_help=False)
    user_sub_mode.add_parser('list', help="List Users", parents=[parent_parser, user_parser], add_help=False)
    user_sub_mode.add_parser('map', help="Map values to users", parents=[parent_parser, user_parser], add_help=False)
    parameters = main_parser.parse_args()

    if parameters.command == 'database':
        sgdb = sg_database(parameters.host, parameters.user, parameters.password)
        if parameters.db_command == "create":
            if not parameters.name:
                parameters.name = parameters.bucket
            sgdb.create(parameters.bucket, parameters.name, parameters.replicas)
        elif parameters.db_command == "delete":
            sgdb.delete(parameters.name)
        elif parameters.db_command == "sync":
            if parameters.get:
                sgdb.get_sync_fun(parameters.name)
            else:
                sgdb.sync_fun(parameters.name, parameters.function)
                sgdb.resync(parameters.name)
        elif parameters.db_command == 'resync':
            sgdb.resync(parameters.name)
        elif parameters.db_command == "list":
            sgdb.list(parameters.name)
        elif parameters.db_command == "dump":
            sgdb.dump(parameters.name)
        elif parameters.db_command == "wait":
            sgdb.ready_wait(parameters.name)
    elif parameters.command == 'user':
        sguser = sg_user(parameters.host, parameters.user, parameters.password)
        if parameters.user_command == "create":
            sguser.create(parameters.name, parameters.sguser, parameters.sgpass)
        elif parameters.user_command == "delete":
            sguser.delete(parameters.name, parameters.sguser)
        elif parameters.user_command == "list":
            if parameters.all:
                sguser.list(parameters.name)
            else:
                sguser.list(parameters.name, parameters.sguser)
        elif parameters.user_command == "map":
            dbuser = parameters.dblogin.split(':')[0]
            dbpass = parameters.dblogin.split(':')[1]
            cbdb = cb_interface(parameters.dbhost, dbuser, dbpass)
            usernames = cbdb.get_values(parameters.field, parameters.keyspace)
            for username in usernames:
                sguser.create(parameters.name, username, parameters.sgpass, channels=f"channel.{username}")


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
