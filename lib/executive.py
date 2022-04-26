##
##

import multiprocessing
import logging
import logging.handlers
from queue import Empty
from lib.cbutil.cbconnect import cb_connect
from lib.cbutil.cbindex import cb_index
from lib.cbutil.randomize import randomize, fastRandom
from lib.inventorymgr import inventoryManager
from lib.cbutil.exceptions import *
from lib.exceptions import *
from lib.cbutil.cbdebug import cb_debug
import json
import os
import numpy as np
import asyncio
import time
import traceback
import signal

VERSION = '1.0'


class rwMixer(object):

    def __init__(self, x=100):
        percentage = x / 100
        if percentage > 0:
            self.factor = 1 / percentage
        else:
            self.factor = 0

    def write(self, n=1):
        if self.factor > 0:
            remainder = n % self.factor
        else:
            remainder = 1
        if remainder == 0:
            return True
        else:
            return False

    def read(self, n=1):
        if self.factor > 0:
            remainder = n % self.factor
        else:
            remainder = 1
        if remainder != 0:
            return True
        else:
            return False


class cbPerfBase(object):

    def __init__(self, parameters):
        config_file, schema_file = self.locate_config_files()
        self.debug = None
        self.settings = {}
        self.playbooks = {}
        self.inventory = None
        self.parameters = parameters
        self.username = None
        self.password = None
        self.host = None
        self.tls = True
        self.external_network = False
        self.aio = True
        self.input_file = None
        self.query_field = None
        self.create_bucket = True
        self.default_operation_count = None
        self.default_record_count = None
        self.default_kv_batch_size = None
        self.default_query_batch_size = None
        self.default_id_field = None
        self.default_bucket_memory = 256
        self.run_threads = os.cpu_count()
        self.thread_max = 512
        self.replica_count = 1
        self.rule_list = None
        self.collection_list = None
        self.rules_run = False
        self.skip_init = False
        self.test_playbook = "default"
        self.read_config_file(config_file)
        self.read_schema_file(schema_file)
        self.record_count = self.default_operation_count
        self.operation_count = self.default_operation_count
        self.bucket_memory = None
        self.session_cache = None
        self.id_field = self.default_id_field
        self.log_file = os.environ.get("CB_PERF_LOGFILE", "cb_perf.log")

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
            self.default_bucket_memory = self.settings['bucket_memory']
        except KeyError:
            raise ConfigFileError("can not read default settings")

    def read_schema_file(self, filepath):
        if not os.path.exists(filepath):
            raise SchemaFileError("can not locate schema file {}".format(filepath))

        try:
            with open(filepath, 'r') as schemafile:
                inventory_data = json.load(schemafile)
                self.inventory = inventoryManager(inventory_data, self.parameters)
                schemafile.close()
        except KeyError:
            raise SchemaFileError("can not read schema file {}".format(filepath))
        except Exception as err:
            raise SchemaFileError("can not open schema file {}: {}".format(filepath, err))

    def get_next_task(self):
        try:
            for item in self.playbooks[self.test_playbook]:
                yield item, self.playbooks[self.test_playbook][item]
        except KeyError:
            raise TestConfigError("test scenario syntax error")


class schema_admin(cbPerfBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self):
        if self.parameters.list:
            self.schema_list()

    def schema_list(self):
        for name in self.inventory.schemaList:
            print(f" {name}")


class print_host_map(cbPerfBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.parameters.user:
            self.username = self.parameters.user
        if self.parameters.password:
            self.password = self.parameters.password
        if self.parameters.tls:
            self.tls = self.parameters.tls
        if self.parameters.host:
            self.host = self.parameters.host
        if self.parameters.external:
            self.external_network = self.parameters.external
        if self.parameters.ping:
            self.cluster_ping = self.parameters.ping
        else:
            self.cluster_ping = False

    def run(self):
        db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network)
        db.print_host_map()
        if self.cluster_ping:
            print("Cluster Status:")
            db.cluster_health_check(output=True, restrict=False)


class test_exec(cbPerfBase):
    KV_TEST = 0x01
    QUERY_TEST = 0x02
    REMOVE_DATA = 0x04
    RANDOM_KEYS = 0x08

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        debugger = cb_debug(self.__class__.__name__, overwrite=True)
        self.logger = debugger.logger
        self.throughput = None
        if self.parameters.user:
            self.username = self.parameters.user
        if self.parameters.password:
            self.password = self.parameters.password
        if self.parameters.tls:
            self.tls = self.parameters.tls
        if self.parameters.host:
            self.host = self.parameters.host
        if self.parameters.external:
            self.external_network = self.parameters.external
        if self.parameters.count:
            self.record_count = self.parameters.count
        if self.parameters.ops:
            self.operation_count = self.parameters.ops
        if self.parameters.threads:
            self.run_threads = self.parameters.threads
        if self.parameters.memquota:
            self.bucket_memory = self.parameters.memquota
        if self.parameters.file:
            self.input_file = self.parameters.file
        if self.parameters.id:
            self.id_field = self.parameters.id
        if self.parameters.debug:
            self.debug = self.parameters.debug
        if self.parameters.skiprules:
            self.run_rules = False
        else:
            self.run_rules = True
        if self.parameters.sync:
            self.aio = False
            self.batchSize = 1
        if self.input_file:
            self.schema = "external_file"
        else:
            self.schema = self.parameters.schema
        if self.parameters.noinit:
            self.skip_init = True

        if self.operation_count > self.record_count:
            raise ParameterError("Error: Operation count must be equal or less than record count.")

        if self.parameters.command == 'load':
            self.test_playbook = "load"
        elif self.parameters.command == 'run' and self.parameters.ramp:
            self.test_playbook = "ramp"

        if not self.bucket_memory:
            one_mb = 1024 * 1024
            if self.input_file:
                try:
                    input_data_size = os.path.getsize(self.input_file)
                except Exception as err:
                    raise TestExecError("Can not get input file size: {}".format(err))
                total_data = input_data_size * self.record_count
                total_data_mb = round(total_data / one_mb)
            else:
                total_data_mb = self.default_bucket_memory

            if total_data_mb < 256:
                total_data_mb = 256

            self.bucket_memory = total_data_mb

    def test_mask(self, bits):
        if bits & test_exec.KV_TEST:
            return test_exec.KV_TEST
        elif bits & test_exec.QUERY_TEST:
            return test_exec.QUERY_TEST
        elif bits & test_exec.REMOVE_DATA:
            return test_exec.REMOVE_DATA
        else:
            return 0x0

    def test_lookup(self, name):
        if name == "KV_TEST":
            return test_exec.KV_TEST
        elif name == "QUERY_TEST":
            return test_exec.QUERY_TEST
        elif name == "REMOVE_DATA":
            return test_exec.REMOVE_DATA
        else:
            raise TestConfigError("unknown test type {}".format(name))

    def test_print_name(self, code):
        if code == test_exec.KV_TEST:
            return "key-value"
        elif code == test_exec.QUERY_TEST:
            return "query"
        elif code == test_exec.REMOVE_DATA:
            return "remove"
        else:
            raise TestConfigError("unknown test code {}".format(code))

    def is_random_mask(self, bits):
        if bits & test_exec.RANDOM_KEYS:
            return True
        else:
            return False

    def format_bytes(self, value):
        labels = ["Bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"]
        value = round(value)
        last_label = labels[-1]
        unit = None

        is_negative = value < 0
        if is_negative:
            value = abs(value)

        for unit in labels:
            if value < 1024:
                break
            if unit != last_label:
                value /= 1024

        value = round(value)
        if value == 1024:
            value = 1
        return f"{value} {unit}"

    def run(self):
        for index, element in enumerate(self.get_next_task()):
            step = element[0]
            step_config = element[1]
            print(f"Running test playbook {self.test_playbook} step {step}")

            try:
                write_p = step_config['write']
                test_type = self.test_lookup(step_config['test'])
                test_pause = step_config['pause']
            except KeyError:
                raise TestConfigError("test configuration syntax error")

            if step == "load" and not self.skip_init:
                self.test_init()
                self.test_bandwidth()

            try:
                read_p = 100 - write_p
                if self.test_playbook == "ramp" and step != "load":
                    self.ramp_launch(read_p=read_p, write_p=write_p, mode=test_type)
                else:
                    self.test_launch(read_p=read_p, write_p=write_p, mode=test_type)
            except Exception as err:
                self.logger.error(f"test launch error: {err}")

            if test_pause:
                self.pause_test()

            if step == "load" and not self.skip_init:
                if self.run_rules:
                    self.process_rules()
                self.check_indexes()

    def write_cache(self):
        try:
            db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network)
            self.session_cache = db.session_cache
        except Exception as err:
            raise TestExecError(f"can not initialize db connection: {err}")

    def test_bandwidth(self):
        key = 1
        max_passes = 5
        r = randomize()
        test_a = {}
        total_throughput = 0

        collection = self.collection_list[0]

        try:
            db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network, restore=self.session_cache)
            db.connect_s()
            db.bucket_s(collection.bucket)
            db.scope_s('_default')
            db.create_collection('bandwidth')
        except Exception as err:
            raise TestExecError(f"bandwidth test: can not connect to cluster: {err}")

        print("Calculating network bandwidth (this may take a minute)")

        for i in range(max_passes):
            doc_size = 1024
            last_value = 0
            while True:
                test_a['data1'] = r._randomHash(doc_size)
                data_size = len(json.dumps(test_a))
                db.cb_upsert_s(key, test_a, name='bandwidth')
                begin_time = time.time()
                db.cb_get_s(key, name='bandwidth')
                end_time = time.time()
                time_diff = end_time - begin_time

                throughput = data_size / time_diff
                doc_size = doc_size * 2

                diff_factor = last_value / throughput

                if diff_factor >= .9:
                    total_throughput += throughput
                    break
                else:
                    last_value = throughput

        average_throughput = round(total_throughput / max_passes)
        print(f"[i] Calculated bandwidth: {self.format_bytes(average_throughput)}/s")

        try:
            db.drop_collection('bandwidth')
        except Exception as err:
            raise TestExecError(f"bandwidth test: can not drop collection: {err}")

        self.throughput = average_throughput
        self.logger.debug(f"calculated bandwidth as {self.format_bytes(average_throughput)}/s")

    def test_clean(self):
        if not self.session_cache:
            self.write_cache()

        db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network, restore=self.session_cache)
        db.connect_s()

        print(f"Running cleanup for schema {self.schema}")

        try:
            schema = self.inventory.getSchema(self.schema)
            if schema:
                for bucket in self.inventory.nextBucket(schema):
                    print(f"Dropping bucket {bucket.name}")
                    if db.is_bucket(bucket.name):
                        db.drop_bucket(bucket.name)
        except Exception as err:
            raise TestExecError(f"cleanup failed: {err}")

    def test_init(self, bypass=False):
        loop = asyncio.get_event_loop()
        collection_list = []
        rule_list = []
        tasks = []

        if not self.session_cache:
            self.write_cache()

        db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network, restore=self.session_cache)
        db.connect_s()
        db_index = cb_index(self.host, self.username, self.password, self.tls, self.external_network, restore=self.session_cache)
        loop.run_until_complete(db_index.connect())
        cluster_memory = db.get_memory_quota
        if cluster_memory < self.bucket_memory:
            print("Warning: requested memory %s MiB less than available memory" % self.bucket_memory)
            print("Adjusting bucket memory to %s MiB" % cluster_memory)
            self.bucket_memory = cluster_memory

        print("Running initialize phase for schema {}".format(self.schema))
        try:
            schema = self.inventory.getSchema(self.schema)
            if schema:
                for bucket in self.inventory.nextBucket(schema):
                    if self.create_bucket and not bypass:
                        print("Creating bucket {}".format(bucket.name))
                        db.create_bucket(bucket.name, quota=self.bucket_memory)
                        db.bucket_wait(bucket.name)
                        loop.run_until_complete(db_index.connect_bucket(bucket.name))
                    for scope in self.inventory.nextScope(bucket):
                        if scope.name != '_default' and not bypass:
                            print("Creating scope {}".format(scope.name))
                            db.create_scope(scope.name)
                            db.scope_wait(scope.name)
                            loop.run_until_complete(db_index.connect_scope(scope.name))
                        elif not bypass:
                            loop.run_until_complete(db_index.connect_scope('_default'))
                        for collection in self.inventory.nextCollection(scope):
                            if collection.name != '_default' and not bypass:
                                print("Creating collection {}".format(collection.name))
                                db.create_collection(collection.name)
                                db.collection_wait(collection.name)
                                loop.run_until_complete(db_index.connect_collection(collection.name))
                            elif not bypass:
                                loop.run_until_complete(db_index.connect_collection('_default'))
                            if self.inventory.hasPrimaryIndex(collection) and not bypass:
                                print(f"Creating primary index on {collection.name}")
                                tasks.append(loop.create_task(db_index.create_index(collection.name, replica=self.replica_count)))
                            if self.inventory.hasIndexes(collection) and not bypass:
                                for index_field, index_name in self.inventory.nextIndex(collection):
                                    print(f"Creating index {index_name} on {index_field}")
                                    tasks.append(loop.create_task(
                                        db_index.create_index(collection.name, field=index_field, index_name=index_name, replica=self.replica_count)))
                            collection_list.append(collection)
                loop.run_until_complete(asyncio.gather(*tasks))
                if self.inventory.hasRules(schema):
                    for rule in self.inventory.nextRule(schema):
                        rule_list.append(rule)
                self.rule_list = rule_list
                self.collection_list = collection_list
            else:
                raise ParameterError("Schema {} not found".format(self.schema))
        except Exception as err:
            # raise
            raise TestExecError("Initialization failed: {}".format(err))

    def list_block(self, list_data, count):
        for i in range(0, len(list_data), count):
            yield list_data[i:i + count]

    def run_link_rule(self, foreign_keyspace, primary_keyspace):
        loop = asyncio.get_event_loop()
        primary_key_list = []
        end_char = '\r'

        db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network, restore=self.session_cache)

        if len(foreign_keyspace) != 4 and len(primary_keyspace) != 4:
            raise RulesError("runLinkRule: link rule key syntax incorrect")

        foreign_bucket, foreign_scope, foreign_collection, foreign_field = foreign_keyspace
        primary_bucket, primary_scope, primary_collection, primary_field = primary_keyspace

        if foreign_bucket != primary_bucket:
            raise RulesError("cross bucket linking is not supported")

        if foreign_scope != primary_scope:
            raise RulesError("cross scope linking is not supported")

        try:
            self.logger.debug("run_link_rule: connecting to database")
            loop.run_until_complete(db.connect_a())
        except Exception as err:
            raise RulesError(f"link: can not connect to database: {err}")

        try:
            loop.run_until_complete(db.bucket_a(primary_bucket))
            loop.run_until_complete(db.scope_a(primary_scope))
            loop.run_until_complete(db.collection_a(foreign_collection))
            loop.run_until_complete(db.collection_a(primary_collection))
        except Exception as err:
            raise RulesError(f"link: can not connect to database: {err}")

        print(f" [1] Building primary key list")

        result = loop.run_until_complete(db.cb_query_a(field=primary_field, name=primary_collection))

        for row in result:
            primary_key_list.append(row[primary_field])

        foreign_record_count = loop.run_until_complete(db.collection_count_a(foreign_collection))

        if foreign_record_count != len(primary_key_list):
            raise RulesError("runLinkRule: primary and foreign record counts are unequal")

        print(f" [2] Inserting keys into field {foreign_field} in {foreign_collection}")
        total_inserted = 0
        for sub_list in list(self.list_block(primary_key_list, self.default_kv_batch_size)):
            total_inserted += len(sub_list)
            progress = round((total_inserted / foreign_record_count) * 100)
            print(f"     {progress}%", end=end_char)
            loop.run_until_complete(db.cb_subdoc_multi_upsert_a(sub_list, foreign_field, sub_list, name=foreign_collection))
        sys.stdout.write("\033[K")
        print("Done.")

    def process_rules(self):
        print("[i] Processing rules.")

        for rule in self.rule_list:
            rule_name = rule['name']
            if rule['type'] == 'link':
                print("[i] Processing rule %s type link" % rule_name)
                foreign_keyspace = rule['foreign_key'].split(':')
                primary_keyspace = rule['primary_key'].split(':')
                try:
                    self.run_link_rule(foreign_keyspace, primary_keyspace)
                except Exception as err:
                    raise RulesError("link rule failed: {}".format(err))

        self.rules_run = True

    def check_indexes(self):
        loop = asyncio.get_event_loop()
        end_char = ''
        print("Waiting for indexes to settle")
        try:
            db_index = cb_index(self.host, self.username, self.password, self.tls, self.external_network, restore=self.session_cache)
            loop.run_until_complete(db_index.connect())
            schema = self.inventory.getSchema(self.schema)
            if schema:
                for bucket in self.inventory.nextBucket(schema):
                    loop.run_until_complete(db_index.connect_bucket(bucket.name))
                    for scope in self.inventory.nextScope(bucket):
                        loop.run_until_complete(db_index.connect_scope(scope.name))
                        for collection in self.inventory.nextCollection(scope):
                            loop.run_until_complete(db_index.connect_collection(collection.name))
                            if self.inventory.hasPrimaryIndex(collection):
                                print(f"Waiting for primary index on {collection.name} ...", end=end_char)
                                loop.run_until_complete(db_index.index_wait(name=collection.name))
                                print("done.")
                            if self.inventory.hasIndexes(collection):
                                for index_field, index_name in self.inventory.nextIndex(collection):
                                    print(f"Waiting for index {index_name} on field {index_field} in keyspace {db_index.db.keyspace_a(collection.name)} ...", end=end_char)
                                    loop.run_until_complete(db_index.index_wait(name=collection.name, field=index_field, index_name=index_name))
                                    print("done.")
            else:
                raise ParameterError("Schema {} not found".format(self.schema))
        except Exception as err:
            raise TestExecError("index check failed: {}".format(err))

    def pause_test(self):
        end_char = ''
        try:
            print("Pausing to check cluster health ...", end=end_char)
            time.sleep(0.5)
            db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network, restore=self.session_cache)
            db.cluster_health_check()
            print("done.")
        except Exception as err:
            raise TestPauseError(f"cluster health not ok: {err}")

    def status_output(self, total_count, run_flag, telemetry_queue, status_vector):
        max_threads = self.thread_max if total_count == 0 else self.run_threads
        tps_vector = [0 for n in range(max_threads)]
        tps_history = []
        sample_count = 1
        total_tps = 0
        total_time = 0
        max_tps = 0
        max_time = 0
        avg_tps = 0
        avg_time = 0
        percentage = 0
        total_ops = 0
        slope_window = 100
        slope_count = 0
        slope_total = 0
        slope_avg = 0
        end_char = '\r'

        def calc_slope(idx, data, segment):
            _idx = np.concatenate(([0], idx))
            _data = np.concatenate(([0], data))
            sum_idx = np.cumsum(_idx)
            sum_data = np.cumsum(_data)
            exp_idx = np.cumsum(_idx * _idx)
            exp_data = np.cumsum(_idx * _data)

            sum_idx = sum_idx[segment:] - sum_idx[:-segment]
            sum_data = sum_data[segment:] - sum_data[:-segment]
            exp_idx = exp_idx[segment:] - exp_idx[:-segment]
            exp_data = exp_data[segment:] - exp_data[:-segment]

            return (segment * exp_data - sum_idx * sum_data) / (segment * exp_idx - sum_idx * sum_idx)

        while run_flag.value == 1:
            try:
                entry = telemetry_queue.get(block=False)
            except Empty:
                continue

            telemetry = entry.split(":")
            n = int(telemetry[0])
            n_ops = int(telemetry[1])
            time_delta = float(telemetry[2])

            tps_vector[n] = round(n_ops / time_delta)
            trans_per_sec = sum(tps_vector)
            total_ops += n_ops
            op_time_delta = time_delta / n_ops
            total_tps = total_tps + trans_per_sec
            total_time = total_time + op_time_delta
            avg_tps = total_tps / sample_count
            avg_time = total_time / sample_count
            sample_count += 1

            tps_history.append(trans_per_sec)
            if len(tps_history) >= slope_window:
                tps_history = tps_history[len(tps_history) - slope_window:len(tps_history)]
                index = list(range(1, len(tps_history)+1))
                np_slope = calc_slope(index, tps_history, slope_window)
                slope = np_slope.tolist()[0]
                slope_total += slope
                slope_count += 1
                slope_avg = slope_total / slope_count

            if trans_per_sec > max_tps:
                max_tps = trans_per_sec
            if time_delta > max_time:
                max_time = time_delta

            if total_count > 0:
                percentage = round((total_ops / total_count) * 100)
                print(f"=> {total_ops} of {total_count}, {status_vector[1]} threads, {time_delta:.6f} time, {trans_per_sec} TPS, {percentage}%",
                      end=end_char)
            else:
                print(f"=> {total_ops} ops, {status_vector[1]} threads, {time_delta:.6f} time, {trans_per_sec} TPS, {status_vector[2]} errors, TPS trend {slope_avg:+.2f}",
                      end=end_char)

        sys.stdout.write("\033[K")
        if total_count > 0:
            print(f"=> {total_count} of {total_count}, {percentage}%")
        print("Test Done.")
        if total_count == 0:
            print(f"{total_ops:,} completed operations")
        print(f"{status_vector[1]} threads")
        print(f"{status_vector[2]} errors")
        print(f"{slope_avg:+.2f} TPS trend")
        print(f"{round(avg_tps):,} average TPS")
        print(f"{round(max_tps):,} maximum TPS")
        print(f"{avg_time:.6f} average time")
        print(f"{max_time:.6f} maximum time")

    def calc_batch_size(self, collection, mode):
        if mode == test_exec.KV_TEST:
            if collection.size and self.throughput:
                total_data = collection.size * self.run_threads
                calc_batch_size = self.throughput / total_data
                new_batch_size = round(calc_batch_size * .7)
                if new_batch_size < self.default_kv_batch_size:
                    print(f"Adjusting batch size due to bandwidth")
                    return new_batch_size
                else:
                    return self.default_kv_batch_size
        elif mode == test_exec.QUERY_TEST:
            return self.default_query_batch_size
        else:
            return 1

    def test_launch(self, read_p=100, write_p=0, mode=KV_TEST):
        if not self.collection_list:
            raise TestRunError("test not initialized")

        run_mode = 'async' if self.aio else 'sync'
        mode_text = self.test_print_name(mode)

        print("Beginning {} {} test with {} instances.".format(run_mode, mode_text, self.run_threads))

        for coll_obj in self.collection_list:
            telemetry_queue = multiprocessing.Queue()
            count = multiprocessing.Value('i')
            run_flag = multiprocessing.Value('i')
            status_flag = multiprocessing.Value('i')
            thread_count = multiprocessing.Value('i')
            status_vector = multiprocessing.Array('i', [0]*10)
            count.value = 0
            run_flag.value = 1
            status_flag.value = 0
            thread_count.value = self.run_threads
            # status_vector[1] = self.run_threads
            input_json = coll_obj.schema

            if coll_obj.record_count:
                operation_count = coll_obj.record_count
            else:
                operation_count = self.record_count

            print(f"Collection {coll_obj.name} document size: {self.format_bytes(coll_obj.size)}")
            coll_obj.batch_size = self.calc_batch_size(coll_obj, mode)
            print(f"Collection {coll_obj.name} mode {mode_text} batch size: {coll_obj.batch_size}")

            status_thread = multiprocessing.Process(target=self.status_output, args=(operation_count, run_flag, telemetry_queue, status_vector))
            status_thread.daemon = True
            status_thread.start()

            print(f"Starting test on {coll_obj.name} with {operation_count:,} records - {read_p}% get, {write_p}% update")
            start_time = time.perf_counter()

            if self.aio:
                test_run_func = self.test_run_a
            else:
                test_run_func = self.test_run_s

            instances = []
            throttle_count = 0
            n = 0
            while True:
                if n == self.run_threads:
                    break

                if not any(p.is_alive() for p in instances) and n > 0:
                    break

                if status_vector[3] < status_vector[1]:
                    if throttle_count == 30:
                        break
                    self.logger.info(f"throttling: {status_vector[1]} requested {status_vector[3]} connected")
                    throttle_count += 1
                    time.sleep(0.5)
                    continue

                throttle_count = 0
                instances.append(multiprocessing.Process(
                    target=test_run_func,
                    args=(mode, input_json, count, coll_obj, operation_count,
                          telemetry_queue, write_p, n, status_vector)))
                instances[n].daemon = True
                instances[n].start()
                status_vector[1] += 1
                n += 1

            for p in instances:
                p.join()

            run_flag.value = 0
            status_thread.join()
            end_time = time.perf_counter()

            print("Test completed in {}".format(
                time.strftime("%H hours %M minutes %S seconds.", time.gmtime(end_time - start_time))))

    def ramp_launch(self, read_p=100, write_p=0, mode=KV_TEST):
        scale = []

        if not self.collection_list:
            raise TestRunError("test not initialized")

        run_mode = 'async' if self.aio else 'sync'
        mask = mode | test_exec.RANDOM_KEYS

        print("Beginning {} test ramp with max {} instances.".format(run_mode, self.thread_max))

        for coll_obj in self.collection_list:
            telemetry_queue = multiprocessing.Queue()
            count = multiprocessing.Value('i')
            run_flag = multiprocessing.Value('i')
            status_flag = multiprocessing.Value('i')
            thread_count = multiprocessing.Value('i')
            status_vector = multiprocessing.Array('i', [0]*10)
            count.value = 0
            run_flag.value = 1
            status_flag.value = 0
            thread_count.value = 0
            input_json = coll_obj.schema
            operation_count = 0
            accelerator = 1
            scale.clear()
            n = 0

            if coll_obj.record_count:
                record_count = coll_obj.record_count
            else:
                record_count = self.record_count

            status_thread = multiprocessing.Process(
                target=self.status_output, args=(operation_count, run_flag, telemetry_queue, status_vector))
            status_thread.daemon = True
            status_thread.start()

            print(f"Starting ramp test - {read_p}% get, {write_p}% update")

            if self.aio:
                test_run_func = self.test_run_a
            else:
                test_run_func = self.test_run_s

            time_snap = time.perf_counter()
            start_time = time_snap
            throttle_count = 0
            while True:
                if status_vector[0] == 1:
                    break

                if n >= self.thread_max:
                    status_vector[0] = 1
                    break

                if status_vector[3] < status_vector[1]:
                    if throttle_count == 30:
                        status_vector[0] = 1
                        break
                    self.logger.info(f"throttling: {status_vector[1]} requested {status_vector[3]} connected")
                    throttle_count += 1
                    time.sleep(0.5)
                    continue

                throttle_count = 0
                status_vector[1] += accelerator
                for i in range(accelerator):
                    scale.append(multiprocessing.Process(
                        target=test_run_func,
                        args=(mask, input_json, count, coll_obj, record_count,
                              telemetry_queue, write_p, n, status_vector)))
                    scale[n].daemon = True
                    scale[n].start()
                    n += 1

                time_check = time.perf_counter()
                time_diff = time_check - time_snap
                if time_diff >= 60:
                    time_snap = time.perf_counter()
                    accelerator *= 2
                time.sleep(5.0)

            for retry in range(10):
                if not any(p.is_alive() for p in scale):
                    break
                time.sleep(0.5)

            for p in scale:
                p.terminate()
                p.join()

            run_flag.value = 0
            status_thread.join()
            end_time = time.perf_counter()

            print("Test completed in {}".format(
                time.strftime("%H hours %M minutes %S seconds.", time.gmtime(end_time - start_time))))

    def test_unhandled_exception(self, loop, context):
        err = context.get("exception", context['message'])
        if isinstance(err, Exception):
            self.logger.error(f"unhandled exception: type: {err.__class__.__name__} msg: {err} cause: {err.__cause__}")
        else:
            self.logger.error(f"unhandled error: {err}")

    def test_run_a(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(self.test_unhandled_exception)
        try:
            loop.run_until_complete(self.async_test_run(*args, **kwargs))
        except Exception as err:
            self.logger.error(f"async test process error: {err}")

    async def async_test_run(self, mask, input_json, count, coll_obj, record_count, telemetry_queue, write_p, n, status_vector):
        tasks = []
        rand_gen = fastRandom(record_count)
        id_field = coll_obj.id
        query_field = next((i['field'] for i in coll_obj.indexes if i['field'] != id_field), id_field)
        op_select = rwMixer(write_p)
        telemetry = [0 for n in range(3)]
        time_threshold = 5
        debugger = cb_debug('async_test_run')
        logger = debugger.logger
        begin_time = time.time()

        logger.info(f"beginning test instance {n}")

        mode = self.test_mask(mask)
        is_random = self.is_random_mask(mask)

        if coll_obj.batch_size:
            run_batch_size = coll_obj.batch_size
        elif mode == test_exec.QUERY_TEST:
            run_batch_size = self.default_query_batch_size
        else:
            run_batch_size = self.default_kv_batch_size

        if status_vector[0] == 1:
            logger.info(f"test_thread_{n:03d}: aborting startup due to stop signal")
            return

        try:
            logger.info(f"test_thread_{n:03d}: connecting to {self.host}")
            db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network, restore=self.session_cache)
            await db.quick_connect_a(coll_obj.bucket, coll_obj.scope, coll_obj.name)
        except Exception as err:
            status_vector[0] = 1
            status_vector[2] += 1
            logger.error(f"test_thread_{n:03d}: db connection error: {err}")
            return

        try:
            r = randomize()
            r.prepareTemplate(input_json)
        except Exception as err:
            status_vector[0] = 1
            status_vector[2] += 1
            logger.error(f"test_thread_{n:03d}: randomizer error: {err}")
            return

        if status_vector[0] == 1:
            logger.info(f"test_thread_{n:03d}: aborting run due to stop signal")
            return

        status_vector[3] += 1
        logger.info(f"test_thread_{n:03d}: commencing run, collection {coll_obj.name} batch size {run_batch_size} mode {mode}")
        if mode == test_exec.QUERY_TEST:
            logger.debug(f"test_thread_{n:03d}: query {query_field} where {id_field} is record number")
        while True:
            try:
                tasks.clear()
                begin_time = time.time()
                for y in range(int(run_batch_size)):
                    if is_random:
                        record_number = rand_gen.value
                    else:
                        with count.get_lock():
                            count.value += 1
                            record_number = count.value
                        if record_number > record_count:
                            break
                    if op_select.write(record_number):
                        document = r.processTemplate()
                        document[self.id_field] = record_number
                        tasks.append(asyncio.create_task(db.cb_upsert_a(record_number, document, name=coll_obj.name)))
                    else:
                        if mode == test_exec.REMOVE_DATA:
                            tasks.append(asyncio.create_task(db.cb_remove_a(record_number, name=coll_obj.name)))
                        elif mode == test_exec.QUERY_TEST:
                            tasks.append(asyncio.create_task(db.cb_query_a(field=query_field, name=coll_obj.name, where=id_field, value=record_number)))
                        else:
                            tasks.append(asyncio.create_task(db.cb_get_a(record_number, name=coll_obj.name)))
            except Exception as err:
                status_vector[0] = 1
                status_vector[2] += 1
                logger.error(f"test_thread_{n:03d}: execution error: {err}")
            if len(tasks) > 0:
                await asyncio.sleep(0)
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        status_vector[0] = 1
                        status_vector[2] += 1
                        logger.error(f"test_thread_{n:03d}: task error #{status_vector[2]}: {result}")
                if status_vector[0] == 1:
                    await asyncio.sleep(0)
                    break
                end_time = time.time()
                loop_total_time = end_time - begin_time
                telemetry[0] = n
                telemetry[1] = len(tasks)
                telemetry[2] = loop_total_time
                telemetry_packet = ':'.join(str(i) for i in telemetry)
                telemetry_queue.put(telemetry_packet)
                if loop_total_time >= time_threshold:
                    status_vector[0] = 1
                    logger.error(f"test_thread_{n:03d}: max latency exceeded")
                    break
            else:
                break

    def test_run_s(self, *args, **kwargs):
        debugger = cb_debug(f"test_run_s")
        logger = debugger.logger
        try:
            self.sync_test_run(*args, **kwargs)
        except Exception as err:
            logger.debug(f"sync test process returned: {err}")

    def sync_test_run(self, mask, input_json, count, coll_obj, record_count, telemetry_queue, write_p, n, status_vector):
        tasks = []
        rand_gen = fastRandom(record_count)
        id_field = coll_obj.id
        query_field = next((i['field'] for i in coll_obj.indexes if i['field'] != id_field), id_field)
        op_select = rwMixer(write_p)
        telemetry = [0 for n in range(3)]
        time_threshold = 5

        self.logger.info(f"beginning test instance {n}")

        mode = self.test_mask(mask)
        is_random = self.is_random_mask(mask)

        if coll_obj.batch_size:
            run_batch_size = coll_obj.batch_size
        elif mode == test_exec.QUERY_TEST:
            run_batch_size = self.default_query_batch_size
        else:
            run_batch_size = self.default_kv_batch_size

        if status_vector[0] == 1:
            self.logger.info(f"test_thread_{n:03d}: aborting startup due to stop signal")
            return

        try:
            self.logger.info(f"test_thread_{n:03d}: connecting to {self.host}")
            db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network, restore=self.session_cache)
            db.quick_connect_s(coll_obj.bucket, coll_obj.scope, coll_obj.name)
        except Exception as err:
            status_vector[0] = 1
            status_vector[2] += 1
            self.logger.info(f"test_thread_{n:03d}: db connection error: {err}")
            return

        try:
            r = randomize()
            r.prepareTemplate(input_json)
        except Exception as err:
            status_vector[0] = 1
            status_vector[2] += 1
            self.logger.info(f"test_thread_{n:03d}: randomizer error: {err}")
            return

        if status_vector[0] == 1:
            self.logger.info(f"test_thread_{n:03d}: aborting run due to stop signal")
            return

        status_vector[3] += 1
        self.logger.info(f"test_thread_{n:03d}: commencing run")
        while True:
            try:
                tasks.clear()
                begin_time = time.time()
                for y in range(int(run_batch_size)):
                    if is_random:
                        record_number = rand_gen.value
                    else:
                        with count.get_lock():
                            count.value += 1
                            record_number = count.value
                        if record_number > record_count:
                            break
                    if op_select.write(record_number):
                        document = r.processTemplate()
                        document[self.id_field] = record_number
                        result = db.cb_upsert_s(record_number, document, name=coll_obj.name)
                        tasks.append(result)
                    else:
                        if mode == test_exec.REMOVE_DATA:
                            result = db.cb_remove_s(record_number, name=coll_obj.name)
                            tasks.append(result)
                        elif mode == test_exec.QUERY_TEST:
                            result = db.cb_query_s(field=query_field, name=coll_obj.name, where=id_field, value=record_number)
                            tasks.append(result)
                        else:
                            result = db.cb_get_s(record_number, name=coll_obj.name)
                            tasks.append(result)
            except Exception as err:
                status_vector[0] = 1
                status_vector[2] += 1
                self.logger.info(f"test_thread_{n:03d}: task error #{status_vector[2]}: {err}")
                break
            if len(tasks) > 0:
                end_time = time.time()
                loop_total_time = end_time - begin_time
                telemetry[0] = n
                telemetry[1] = len(tasks)
                telemetry[2] = loop_total_time
                telemetry_packet = ':'.join(str(i) for i in telemetry)
                telemetry_queue.put(telemetry_packet)
                if loop_total_time >= time_threshold:
                    status_vector[0] = 1
                    self.logger.info(f"test_thread_{n:03d}: max latency exceeded")
                    break
            else:
                break
