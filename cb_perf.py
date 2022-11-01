#!/usr/bin/env python3

'''
Couchbase Performance Utility
'''

import argparse
import signal
import warnings
import traceback
import logging
from lib.executive import print_host_map, test_exec, schema_admin
from lib.exceptions import *


LOAD_DATA = 0x0000
KV_TEST = 0x0001
QUERY_TEST = 0x0002
REMOVE_DATA = 0x0003
PAUSE_TEST = 0x0009
INSTANCE_MAX = 0x200
RUN_STOP = 0xFFFF
VERSION = '2.0b2'

warnings.filterwarnings("ignore")
logger = logging.getLogger()


def break_signal_handler(signum, frame):
    if 'CB_PERF_DEBUG_LEVEL' in os.environ:
        if int(os.environ['CB_PERF_DEBUG_LEVEL']) == 0:
            tb = traceback.format_exc()
            print(tb)
    print("")
    print("Break received, aborting.")
    sys.exit(1)


def int_arg(value):
    try:
        return int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("numeric argument expected")


class params(object):

    def __init__(self):
        parser = argparse.ArgumentParser(add_help=False)
        parent_parser = argparse.ArgumentParser(add_help=False)
        parent_parser.add_argument('-u', '--user', action='store', help="User Name", default="Administrator")
        parent_parser.add_argument('-p', '--password', action='store', help="User Password", default="password")
        parent_parser.add_argument('-h', '--host', action='store', help="Cluster Node Name", default="localhost")
        parent_parser.add_argument('-b', '--bucket', action='store', help="Test Bucket", default="pillowfight")
        parent_parser.add_argument('--tls', action='store_true', help="Enable SSL")
        parent_parser.add_argument('--debug', action='store', help="Enable Debug Output", type=int_arg, default=3)
        parent_parser.add_argument('--limit', action='store_true', help="Limited Network Connectivity")
        parent_parser.add_argument('--noapi', action='store_true', help="Disable Capella API functionality")
        parent_parser.add_argument('--safe', action='store_true', help="Do not overwrite data")
        parent_parser.add_argument('-e', '--external', action='store_true', help='Use external network')
        parent_parser.add_argument('--file', action='store', help="File based collection schema JSON")
        parent_parser.add_argument('--schema', action='store', help="Test Schema")
        parent_parser.add_argument('--help', action='help', default=argparse.SUPPRESS, help='Show help message')
        list_parser = argparse.ArgumentParser(add_help=False)
        list_parser.add_argument('--ping', action='store_true', help='Show cluster ping output')
        list_parser.add_argument('--test', action='store_true', help='Just check status and error if not ready')
        list_parser.add_argument('--wait', action='store_true', help='Wait for cluster to be ready')
        schema_parser = argparse.ArgumentParser(add_help=False)
        schema_parser.add_argument('--list', action='store_true', help='Show schema list')
        schema_parser.add_argument('--help', action='help', default=argparse.SUPPRESS, help='Show help message')
        run_parser = argparse.ArgumentParser(add_help=False)
        # run_parser.add_argument('--schema', action='store', help="Test Schema", default="default")
        # run_parser.add_argument('--cluster', action='store', help="Couchbase Capella Cluster Name")
        run_parser.add_argument('--count', action='store', help="Record Count", type=int_arg)
        run_parser.add_argument('--ops', action='store', help="Operation Count", type=int_arg)
        run_parser.add_argument('--threads', action='store', help="Threads for run", type=int_arg)
        run_parser.add_argument('--replica', action='store', help="Replica Count", type=int_arg, default=1)
        run_parser.add_argument('--memquota', action='store', help="Bucket Memory Quota", type=int_arg)
        # run_parser.add_argument('--file', action='store', help="File based collection schema JSON")
        run_parser.add_argument('--output', action='store', help="Output file for run stats")
        run_parser.add_argument('--inventory', action='store', help="Location of inventory JSON")
        run_parser.add_argument('--id', action='store', help="ID field for file based collection schema", default="record_id")
        # run_parser.add_argument('--query', action='store', help="Field to query in JSON File", default="last_name")
        # run_parser.add_argument('--load', action='store_true', help="Only Load Data")
        run_parser.add_argument('--max', action='store', help="Max ramp threads", type=int_arg)
        run_parser.add_argument('--ramp', action='store_true', help="Run Calibration Style Test")
        run_parser.add_argument('--sync', action='store_true', help="Use Synchronous Connections")
        run_parser.add_argument('--noinit', action='store_true', help="Skip init phase")
        run_parser.add_argument('--skipbucket', action='store_true', help="Use Preexisting bucket")
        run_parser.add_argument('--skiprules', action='store_true', help="Do not run rules if defined")
        subparsers = parser.add_subparsers(dest='command')
        run_mode = subparsers.add_parser('run', help="Run Test Scenarios", parents=[parent_parser, run_parser], add_help=False)
        list_mode = subparsers.add_parser('list', help="List Nodes", parents=[parent_parser, list_parser], add_help=False)
        clean_mode = subparsers.add_parser('clean', help="Clean Up", parents=[parent_parser, run_parser], add_help=False)
        load_mode = subparsers.add_parser('load', help="Load Data", parents=[parent_parser, run_parser], add_help=False)
        schema_mode = subparsers.add_parser('schema', help="Schema Admin", parents=[schema_parser], add_help=False)
        self.parser = parser
        self.run_parser = run_mode
        self.list_parser = list_mode
        self.clean_parser = clean_mode
        self.load_parser = load_mode
        self.schema_parser = schema_mode


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
        elif self.verb == 'schema':
            task = schema_admin(self.args)
            task.run()
            sys.exit(0)
        elif self.verb == 'clean':
            task = test_exec(self.args)
            task.test_clean()
            sys.exit(0)
        else:
            task = test_exec(self.args)
            task.run()


def main():
    global logger
    arg_parser = params()
    parameters = arg_parser.parser.parse_args()
    signal.signal(signal.SIGINT, break_signal_handler)
    default_debug_file = 'cb_debug.log'
    debug_file = os.environ.get("CB_PERF_DEBUG_FILE", default_debug_file)

    try:
        open(debug_file, 'w').close()
    except Exception as err:
        print(f"[!] Warning: can not clear log file {debug_file}: {err}")

    handler = logging.FileHandler(debug_file)
    formatter = logging.Formatter(logging.BASIC_FORMAT)
    handler.setFormatter(formatter)

    try:
        debug_level = int(os.environ['CB_PERF_DEBUG_LEVEL'])
    except (ValueError, KeyError):
        debug_level = 2

    if debug_level == 0:
        logger.setLevel(logging.DEBUG)
    elif debug_level == 1:
        logger.setLevel(logging.INFO)
    elif debug_level == 2:
        logger.setLevel(logging.ERROR)
    else:
        logger.setLevel(logging.CRITICAL)

    logger.addHandler(handler)

    test_run = cbPerf(parameters)
    test_run.run()


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
