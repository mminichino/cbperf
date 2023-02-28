##
##

import os
import warnings
import argparse
from enum import Enum
from lib.schema import ProcessSchema, ProcessVariables


warnings.filterwarnings("ignore")
lib_dir = os.path.dirname(os.path.realpath(__file__))
package_dir = os.path.dirname(lib_dir)


class OperatingMode(Enum):
    LOAD = 0
    READ = 1
    LIST = 2


if 'HOME' in os.environ:
    home_dir = os.environ['HOME']
else:
    home_dir = '/var/tmp'

if os.getenv('CBPERF_CONFIG_FILE'):
    config_file = os.getenv('CBPERF_CONFIG_FILE')
elif os.path.exists("config.json"):
    config_file = "config.json"
elif os.path.exists('config/config.json'):
    config_file = 'config/config.json'
elif os.path.exists("/etc/cbperf/config.json"):
    config_file = "/etc/cbperf/config.json"
elif os.path.exists(f"{package_dir}/config/config.json"):
    schema_file = f"{package_dir}/config/config.json"
else:
    config_file = home_dir + '/.cbperf/config.json'

if os.getenv('CBPERF_SCHEMA_FILE'):
    schema_file = os.getenv('CBPERF_SCHEMA_FILE')
elif os.path.exists("schema.json"):
    schema_file = "schema.json"
elif os.path.exists('schema/schema.json'):
    schema_file = 'schema/schema.json'
elif os.path.exists("/etc/cbperf/schema.json"):
    schema_file = "/etc/cbperf/schema.json"
elif os.path.exists(f"{package_dir}/schema/schema.json"):
    schema_file = f"{package_dir}/schema/schema.json"
else:
    schema_file = home_dir + '/.cbperf/schema.json'

username = "Administrator"
password = "password"
tls = False
host = "localhost"
external_network = False
default_debug_file = f"{package_dir}/log/cb_debug.log"
debug_file = os.environ.get("CB_PERF_DEBUG_FILE", default_debug_file)
schema_name = None
input_file = None
inventory = None
schema = None
output_file = None
output_dir = None
command = "load"
op_mode = OperatingMode.LOAD.value
continuous = False
batch_size = 100
count = 100
replicas = 0
bucket_name = None
scope_name = None
collection_name = None
document_key = None
insert_data = None
wait_mode = False
ping_mode = False
test_mode = False
schema_file_json = {}
id_key = None


def process_params(parameters: argparse.Namespace) -> None:
    global username, \
        password, \
        tls, \
        host, \
        external_network, \
        schema, \
        input_file, \
        schema_name, \
        inventory, \
        output_file, \
        output_dir, \
        command, \
        op_mode, \
        count, \
        replicas, \
        bucket_name, \
        scope_name, \
        collection_name, \
        document_key, \
        insert_data, \
        wait_mode, \
        ping_mode, \
        test_mode, \
        schema_file_json, \
        id_key

    if parameters.user:
        username = parameters.user
    if parameters.password:
        password = parameters.password
    if parameters.tls:
        tls = parameters.tls
    if parameters.host:
        host = parameters.host
    if parameters.external:
        external_network = parameters.external
    if parameters.file:
        input_file = parameters.file
        schema_file_json = ProcessVariables().read_file(input_file)
    if parameters.id:
        id_key = parameters.id
    if parameters.outfile:
        output_file = parameters.outfile
    if parameters.replica:
        replicas = parameters.replica
    if parameters.bucket:
        bucket_name = parameters.bucket
    if parameters.scope:
        scope_name = parameters.scope
    if parameters.collection:
        collection_name = parameters.collection
    if parameters.key:
        document_key = parameters.key
    if parameters.data:
        insert_data = parameters.data
    if parameters.directory:
        output_dir = parameters.directory
    else:
        output_dir = os.environ['HOME']
    if input_file:
        schema_name = "external_file"
        parameters.schema = "external_file"
    else:
        if parameters.schema:
            schema_name = parameters.schema
    if parameters.command:
        command = parameters.command
    if command == 'load':
        op_mode = OperatingMode.LOAD.value
    elif command == "get":
        op_mode = OperatingMode.READ.value
    elif command == "list":
        op_mode = OperatingMode.LIST.value
    if parameters.count:
        count = parameters.count

    if op_mode == OperatingMode.LIST.value:
        if parameters.wait:
            wait_mode = parameters.wait
        if parameters.ping:
            ping_mode = parameters.ping
        if parameters.test:
            test_mode = parameters.test

    if schema_name:
        inventory = ProcessSchema(schema_file).inventory()
        schema = inventory.get(schema_name)
