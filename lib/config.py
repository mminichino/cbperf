##
##

import os
import warnings
import argparse
from lib.schema import ProcessSchema


warnings.filterwarnings("ignore")
lib_dir = os.path.dirname(os.path.realpath(__file__))
package_dir = os.path.dirname(lib_dir)


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
        output_dir

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
    if parameters.outfile:
        output_file = parameters.outfile
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
        else:
            schema_name = "default"

    inventory = ProcessSchema(schema_file).inventory()
    schema = inventory.get(schema_name)
