#!/usr/bin/env -S python3 -W ignore

'''
Couchbase Performance Utility
'''

import argparse
import signal
from lib.executive import print_host_map, test_exec
from lib.exceptions import *


LOAD_DATA = 0x0000
KV_TEST = 0x0001
QUERY_TEST = 0x0002
REMOVE_DATA = 0x0003
PAUSE_TEST = 0x0009
INSTANCE_MAX = 0x200
RUN_STOP = 0xFFFF
VERSION = '2.0-alpha'


def break_signal_handler(signum, frame):
    print("")
    print("Break received, aborting.")
    sys.exit(1)


class params(object):

    def __init__(self):
        parser = argparse.ArgumentParser(add_help=False)
        parent_parser = argparse.ArgumentParser(add_help=False)
        parent_parser.add_argument('-u', '--user', action='store', help="User Name", default="Administrator")
        parent_parser.add_argument('-p', '--password', action='store', help="User Password", default="password")
        parent_parser.add_argument('-h', '--host', action='store', help="Cluster Node Name", default="localhost")
        parent_parser.add_argument('-b', '--bucket', action='store', help="Test Bucket", default="pillowfight")
        parent_parser.add_argument('--tls', action='store_true', help="Enable SSL")
        parent_parser.add_argument('--debug', action='store', help="Enable Debug Output", type=int, default=3)
        parent_parser.add_argument('--limit', action='store_true', help="Limited Network Connectivity")
        parent_parser.add_argument('--internal', action='store_true', help="Use Default over External Network")
        parent_parser.add_argument('-e', '--external', action='store_true')
        parent_parser.add_argument('--schema', action='store', help="Test Schema", default="default")
        parent_parser.add_argument('--cluster', action='store', help="Couchbase Capella Cluster Name")
        parent_parser.add_argument('--help', action='help', default=argparse.SUPPRESS, help='Show help message')
        run_parser = argparse.ArgumentParser(add_help=False)
        run_parser.add_argument('--count', action='store', help="Record Count", type=int)
        run_parser.add_argument('--ops', action='store', help="Operation Count", type=int)
        run_parser.add_argument('--tload', action='store', help="Threads for Load", type=int)
        run_parser.add_argument('--trun', action='store', help="Threads for Run", type=int)
        run_parser.add_argument('--memquota', action='store', help="Bucket Memory Quota", type=int)
        run_parser.add_argument('--file', action='store', help="Input JSON File")
        run_parser.add_argument('--inventory', action='store', help="Schema JSON File")
        run_parser.add_argument('--id', action='store', help="Numeric ID Field in JSON File", default="record_id")
        run_parser.add_argument('--query', action='store', help="Field to query in JSON File", default="last_name")
        run_parser.add_argument('--load', action='store_true', help="Only Load Data")
        run_parser.add_argument('--dryrun', action='store_true', help="Run Single Record Test Pass")
        run_parser.add_argument('--ramp', action='store_true', help="Run Calibration Style Test")
        run_parser.add_argument('--sync', action='store_true', help="Use Synchronous Connections")
        run_parser.add_argument('--noinit', action='store_true', help="Skip init phase")
        run_parser.add_argument('--skipbucket', action='store_true', help="Use Preexisting bucket")
        subparsers = parser.add_subparsers(dest='command')
        run_mode = subparsers.add_parser('run', help="Run Test Scenarios", parents=[parent_parser, run_parser], add_help=False)
        list_mode = subparsers.add_parser('list', help="List Nodes", parents=[parent_parser], add_help=False)
        clean_mode = subparsers.add_parser('clean', help="Clean Up", parents=[parent_parser], add_help=False)
        load_mode = subparsers.add_parser('load', help="Load Data", parents=[parent_parser, run_parser], add_help=False)
        self.parser = parser
        self.run_parser = run_mode
        self.list_parser = list_mode
        self.clean_parser = clean_mode
        self.load_parser = load_mode


class cbPerf(object):

    def __init__(self, parameters):
        print("CBPerf version %s" % VERSION)
        self.args = parameters
        self.verb = self.args.command

    def run(self):
        if self.verb == 'list':
            task = print_host_map(self.args)
            task.run()
            sys.exit(0)
        else:
            task = test_exec(self.args)
            task.run()


def main():
    arg_parser = params()
    parameters = arg_parser.parser.parse_args()
    signal.signal(signal.SIGINT, break_signal_handler)

    test_run = cbPerf(parameters)
    test_run.run()


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
