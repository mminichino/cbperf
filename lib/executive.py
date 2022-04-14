##
##

from lib.cbutil.cbconnect import cb_connect
from lib.cbutil.cbindex import cb_index
from lib.cbutil.randomize import randomize, fastRandom
from lib.inventorymgr import inventoryManager
from lib.cbutil.exceptions import *
from lib.exceptions import *
import json


class cbPerfBase(object):

    def __init__(self, parameters):
        config_file, schema_file = self.locate_config_files()
        self.settings = {}
        self.playbooks = {}
        self.inventory = {}
        self.parameters = parameters
        self.username = None
        self.password = None
        self.host = None
        self.tls = True
        self.external_network = False
        self.aio = True
        self.default_operation_count = None
        self.default_record_count = None
        self.default_kv_batch_size = None
        self.default_query_batch_size = None
        self.default_id_field = None
        self.read_config_file(config_file)
        self.read_schema_file(schema_file)

        if parameters.user:
            self.username = parameters.user
        if parameters.password:
            self.password = parameters.password
        if parameters.tls:
            self.tls = parameters.tls
        if parameters.host:
            self.host = parameters.host
        if parameters.external:
            self.external_network = parameters.external

    def locate_config_files(self):
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
        else:
            schema_file = home_dir + '/.cbperf/schema.json'

        return config_file, schema_file

    def read_config_file(self, filepath):
        if not os.path.exists(filepath):
            raise ConfigFileError("can not locate config file {}".format(filepath))

        try:
            with open(filepath, 'r') as configfile:
                config_contents = json.load(configfile)
                configfile.close()
        except Exception as err:
            raise ConfigFileError("can not open config file {}: {}".format(filepath, err))

        try:
            self.settings = config_contents['settings']
            self.playbooks = config_contents['playbooks']
            self.username = self.settings['username']
            self.password = self.settings['password']
            self.host = self.settings['hostname']
            self.tls = self.settings['ssl']
            self.external_network = self.settings['external_network']
            self.aio = self.settings['async']
            self.default_operation_count = self.settings['operation_count']
            self.default_record_count = self.settings['record_count']
            self.default_kv_batch_size = self.settings['kv_batch_size']
            self.default_query_batch_size = self.settings['query_batch_size']
            self.default_id_field = self.settings['id_field']
        except KeyError:
            raise ConfigFileError("can not read default settings")

    def read_schema_file(self, filepath):
        if not os.path.exists(filepath):
            raise SchemaFileError("can not locate schema file {}".format(filepath))

        try:
            with open(filepath, 'r') as schemafile:
                self.inventory = json.load(schemafile)
                schemafile.close()
        except KeyError:
            raise SchemaFileError("can not read schema file {}".format(filepath))
        except Exception as err:
            raise SchemaFileError("can not open schema file {}: {}".format(filepath, err))


class print_host_map(cbPerfBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self):
        db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network)
        db.print_host_map()


class data_load(cbPerfBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

