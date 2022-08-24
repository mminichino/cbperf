#!/usr/bin/env python3

import argparse
import sys
import signal
import json
from lib.cbutil.httpsessionmgr import api_session


def break_signal_handler(signum, frame):
    print("")
    print("Break received, aborting.")
    sys.exit(1)


def main():
    signal.signal(signal.SIGINT, break_signal_handler)

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument('-u', '--user', action='store', help="User Name", default="Administrator")
    parent_parser.add_argument('-p', '--password', action='store', help="User Password", default="password")
    parent_parser.add_argument('-n', '--node', action='store', help="Sync Gateway Node Name", default="localhost")
    parent_parser.add_argument('-h', '--help', action='help', default=argparse.SUPPRESS, help='Show help message')
    db_parser = argparse.ArgumentParser(add_help=False)
    db_parser.add_argument('-b', '--bucket', action='store', help='Bucket name')
    db_parser.add_argument('-r', '--replicas', action='store', help='Replica count')
    user_parser = argparse.ArgumentParser(add_help=False)
    user_parser.add_argument('--du', action='store', help='Database user')
    user_parser.add_argument('--dp', action='store',  help='Database password')
    main_parser = argparse.ArgumentParser(add_help=False)
    main_parser.add_argument('-h', '--help', action='help', default=argparse.SUPPRESS, help='Show help message')
    subparser = main_parser.add_subparsers(dest='command')
    db_mode = subparser.add_parser('database', help="Database Operations", parents=[parent_parser, db_parser], add_help=False)
    db_sub_mode = db_mode.add_subparsers(dest='db_command')
    db_sub_mode.add_parser('create', help="Create Database", parents=[parent_parser, db_parser], add_help=False)
    db_sub_mode.add_parser('list', help="List Databases", parents=[parent_parser], add_help=False)
    user_mode = subparser.add_parser('user', help="User Operations", parents=[parent_parser, user_parser], add_help=False)
    user_sub_mode = user_mode.add_subparsers(dest='user_command')
    user_sub_mode.add_parser('create', help="Add User", parents=[parent_parser, user_parser], add_help=False)
    user_sub_mode.add_parser('list', help="List Users", parents=[parent_parser], add_help=False)
    parameters = main_parser.parse_args()

    if parameters.command == 'database':
        print(f"database command: {parameters.db_command}")
    elif parameters.command == 'user':
        print(f"user command {parameters.user_command}")


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
