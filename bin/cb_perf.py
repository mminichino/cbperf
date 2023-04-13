#!/usr/bin/env python3

"""
Couchbase Performance Utility
"""

import argparse
import signal
import warnings
import traceback
from lib.exceptions import *
import lib.config as config
from lib.export import CBExport, ExportType
from lib.pimport import PluginImport
from lib.logging import CustomFormatter
from lib.main import MainLoop
from lib.config import OperatingMode


LOAD_DATA = 0x0000
KV_TEST = 0x0001
QUERY_TEST = 0x0002
REMOVE_DATA = 0x0003
PAUSE_TEST = 0x0009
INSTANCE_MAX = 0x200
RUN_STOP = 0xFFFF
VERSION = '2.0.6'

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


class Params(object):

    def __init__(self):
        parser = argparse.ArgumentParser(add_help=False)
        parent_parser = argparse.ArgumentParser(add_help=False)
        parent_parser.add_argument('-u', '--user', action='store', help="User Name", default="Administrator")
        parent_parser.add_argument('-p', '--password', action='store', help="User Password", default="password")
        parent_parser.add_argument('-h', '--host', action='store', help="Cluster Node Name", default="localhost")
        parent_parser.add_argument('-b', '--bucket', action='store', help="Bucket", default="pillowfight")
        parent_parser.add_argument('-s', '--scope', action='store', help="Scope", default="_default")
        parent_parser.add_argument('-c', '--collection', action='store', help="Collection", default="_default")
        parent_parser.add_argument('-k', '--key', action='store', help="Document Key")
        parent_parser.add_argument('-d', '--data', action='store', help="Document To Insert")
        parent_parser.add_argument('-q', '--quiet', action='store_true', help="Suppress Output")
        parent_parser.add_argument('-i', '--index', action='store_true', help="Create Index")
        parent_parser.add_argument('-O', '--stdout', action='store_true', help="Output to terminal")
        parent_parser.add_argument('-P', '--plugin', action='store', help="Export Plugin")
        parent_parser.add_argument('-V', '--variable', action='append', nargs='+', help="Plugin Variable")
        parent_parser.add_argument('--docid', action='store', help="Import document ID field", default="doc_id")
        parent_parser.add_argument('--tls', action='store_true', help="Enable SSL")
        parent_parser.add_argument('--debug', action='store', help="Enable Debug Output", type=int_arg, default=3)
        parent_parser.add_argument('--limit', action='store_true', help="Limited Network Connectivity")
        parent_parser.add_argument('--noapi', action='store_true', help="Disable Capella API functionality")
        parent_parser.add_argument('--safe', action='store_true', help="Do not overwrite data")
        parent_parser.add_argument('-e', '--external', action='store_true', help='Use external network')
        parent_parser.add_argument('-f', '--file', action='store', help="File based collection schema JSON")
        parent_parser.add_argument('--outfile', action='store', help="Output file", default="output.dat")
        parent_parser.add_argument('--directory', action='store', help="Output directory")
        parent_parser.add_argument('--schema', action='store', help="Test Schema")
        parent_parser.add_argument('--dev', action='store_true', help="Use Experimental Methods")
        parent_parser.add_argument('--help', action='help', default=argparse.SUPPRESS, help='Show help message')
        list_parser = argparse.ArgumentParser(add_help=False)
        list_parser.add_argument('--ping', action='store_true', help='Show cluster ping output')
        list_parser.add_argument('--test', action='store_true', help='Just check status and error if not ready')
        list_parser.add_argument('--wait', action='store_true', help='Wait for cluster to be ready')
        schema_parser = argparse.ArgumentParser(add_help=False)
        schema_parser.add_argument('--list', action='store_true', help='Show schema list')
        schema_parser.add_argument('--help', action='help', default=argparse.SUPPRESS, help='Show help message')
        run_parser = argparse.ArgumentParser(add_help=False)
        run_parser.add_argument('--count', action='store', help="Record Count", type=int_arg)
        run_parser.add_argument('--ops', action='store', help="Operation Count", type=int_arg)
        run_parser.add_argument('--threads', action='store', help="Threads for run", type=int_arg)
        run_parser.add_argument('--replica', action='store', help="Replica Count", type=int_arg, default=1)
        run_parser.add_argument('--memquota', action='store', help="Bucket Memory Quota", type=int_arg)
        run_parser.add_argument('--output', action='store', help="Output file for run stats")
        run_parser.add_argument('--inventory', action='store', help="Location of inventory JSON")
        run_parser.add_argument('--id', action='store', help="ID field for file based collection schema", default="record_id")
        run_parser.add_argument('--max', action='store', help="Max ramp threads", type=int_arg)
        run_parser.add_argument('--ramp', action='store_true', help="Run Calibration Style Test")
        run_parser.add_argument('--sync', action='store_true', help="Use Synchronous Connections")
        run_parser.add_argument('--noinit', action='store_true', help="Skip init phase")
        run_parser.add_argument('--skipbucket', action='store_true', help="Use Preexisting bucket")
        run_parser.add_argument('--skiprules', action='store_true', help="Do not run rules if defined")
        subparsers = parser.add_subparsers(dest='command')
        list_mode = subparsers.add_parser('list', help="List Nodes", parents=[parent_parser, list_parser, run_parser], add_help=False)
        clean_mode = subparsers.add_parser('clean', help="Clean Up", parents=[parent_parser, run_parser], add_help=False)
        load_mode = subparsers.add_parser('load', help="Load Data", parents=[parent_parser, run_parser], add_help=False)
        read_mode = subparsers.add_parser('get', help="Get Data", parents=[parent_parser, run_parser], add_help=False)
        schema_mode = subparsers.add_parser('schema', help="Schema Admin", parents=[parent_parser, run_parser], add_help=False)
        export_mode = subparsers.add_parser('export', help="Export Data", parents=[parent_parser, run_parser], add_help=False)
        export_action = export_mode.add_subparsers(dest='export_command')
        export_action.add_parser('csv', help="Export CSV", parents=[parent_parser, run_parser], add_help=False)
        export_action.add_parser('json', help="Export JSON", parents=[parent_parser, run_parser], add_help=False)
        import_mode = subparsers.add_parser('import', help="Import Data", parents=[parent_parser, run_parser], add_help=False)
        self.parser = parser
        self.list_parser = list_mode
        self.clean_parser = clean_mode
        self.load_parser = load_mode
        self.read_parser = read_mode
        self.schema_parser = schema_mode
        self.export_parser = export_mode
        self.import_parser = import_mode


class CBPerf(object):

    def __init__(self, parameters):
        logger.info("CBPerf version %s" % VERSION)
        self.args = parameters
        self.verb = self.args.command

    def run(self):
        if self.verb == 'list':
            MainLoop().cluster_list()
            sys.exit(0)
        elif self.verb == 'schema':
            MainLoop().schema_list()
            sys.exit(0)
        elif self.verb == 'clean':
            MainLoop().schema_remove()
            sys.exit(0)
        elif self.verb == 'export':
            if self.args.export_command == 'csv':
                CBExport().export(ExportType.csv)
            elif self.args.export_command == 'json':
                CBExport().export(ExportType.json)
            sys.exit(0)
        elif self.verb == 'import':
            PluginImport().import_tables()
            sys.exit(0)
        else:
            if config.op_mode == OperatingMode.LOAD.value and self.args.schema:
                MainLoop().schema_load()
            elif config.op_mode == OperatingMode.LOAD.value:
                MainLoop().input_load()
            elif config.op_mode == OperatingMode.READ.value:
                MainLoop().read()


def main():
    global logger
    arg_parser = Params()
    parameters = arg_parser.parser.parse_args()
    signal.signal(signal.SIGINT, break_signal_handler)

    try:
        debug_level = int(os.environ['CB_PERF_DEBUG_LEVEL'])
    except (ValueError, KeyError):
        debug_level = 2

    if parameters.quiet:
        debug_level = 3

    if debug_level == 0:
        logger.setLevel(logging.DEBUG)

        try:
            open(config.debug_file, 'w').close()
        except Exception as err:
            print(f"[!] Warning: can not clear log file {config.debug_file}: {err}")

        file_handler = logging.FileHandler(config.default_debug_file)
        file_formatter = logging.Formatter(logging.BASIC_FORMAT)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    elif debug_level == 1:
        logger.setLevel(logging.ERROR)
    elif debug_level == 2:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.CRITICAL)

    screen_handler = logging.StreamHandler()
    screen_handler.setFormatter(CustomFormatter())
    logger.addHandler(screen_handler)

    config.process_params(parameters)
    test_run = CBPerf(parameters)
    test_run.run()


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
