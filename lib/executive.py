##
##

import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures
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
from lib.system import sys_info
from lib.testmods import test_mods
from lib.constants import *
import json
import os
import numpy as np
import asyncio
import time
import traceback
import signal

VERSION = '1.0'


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
        self.tls = False
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
        if self.parameters.noapi:
            self.cloud_api = False
        else:
            self.cloud_api = True

    def run(self):
        db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network, cloud=self.cloud_api)
        db.print_host_map()
        if self.cluster_ping:
            print("Cluster Status:")
            db.cluster_health_check(output=True, restrict=False)


class test_exec(cbPerfBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        debugger = cb_debug(self.__class__.__name__)
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
        self.replica_count = self.parameters.replica
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
        if self.parameters.noapi:
            self.cloud_api = False
        else:
            self.cloud_api = True

        if self.parameters.command == 'load':
            self.test_playbook = "load"
            self.operation_count = self.record_count

        elif self.parameters.command == 'run' and self.parameters.ramp:
            self.test_playbook = "ramp"

        if self.operation_count > self.record_count:
            raise ParameterError("Error: Operation count must be equal or less than record count.")

        self.bandwidth_test_flag = False

        try:
            self.loop = asyncio.get_event_loop()
            self.loop.set_exception_handler(self.test_unhandled_exception)
            self.db = self.write_cache()
            self.db_index = cb_index(self.host, self.username, self.password, self.tls, self.external_network, restore=self.session_cache, cloud=self.cloud_api)
            self.db.connect_s()
            self.db_index.connect()
        except Exception as err:
            raise TestExecError(f"test_exec init: {err}")

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

    def test_lookup(self, name):
        if name == "KV_TEST":
            return KV_TEST
        elif name == "QUERY_TEST":
            return QUERY_TEST
        elif name == "REMOVE_DATA":
            return REMOVE_DATA
        else:
            raise TestConfigError("unknown test type {}".format(name))

    def test_print_name(self, code):
        if code == KV_TEST:
            return "key-value"
        elif code == QUERY_TEST:
            return "query"
        elif code == REMOVE_DATA:
            return "remove"
        else:
            raise TestConfigError("unknown test code {}".format(code))

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
                if self.bandwidth_test_flag:
                    self.test_bandwidth()

            try:
                read_p = 100 - write_p
                if self.test_playbook == "ramp" and step != "load":
                    self.ramp_launch(read_p=read_p, write_p=write_p, mode=test_type)
                else:
                    if self.aio:
                        self.test_launch_a(read_p=read_p, write_p=write_p, mode=test_type)
                    else:
                        self.test_launch_s(read_p=read_p, write_p=write_p, mode=test_type)
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
            db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network, cloud=self.cloud_api)
            self.session_cache = db.session_cache
            return db
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
            self.db.bucket_s(collection.bucket)
            self.db.scope_s('_default')
            self.db.create_collection('bandwidth')
        except Exception as err:
            raise TestExecError(f"bandwidth test: can not connect to cluster: {err}")

        print("Calculating network bandwidth (this may take a minute)")

        for i in range(max_passes):
            doc_size = 1024
            last_value = 0
            while True:
                test_a['data1'] = r._randomHash(doc_size)
                data_size = len(json.dumps(test_a))
                self.db.cb_upsert_s(key, test_a, name='bandwidth')
                begin_time = time.time()
                self.db.cb_get_s(key, name='bandwidth')
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
            self.db.drop_collection('bandwidth')
        except Exception as err:
            raise TestExecError(f"bandwidth test: can not drop collection: {err}")

        self.throughput = average_throughput
        self.logger.debug(f"calculated bandwidth as {self.format_bytes(average_throughput)}/s")

    def test_clean(self):
        print(f"Running cleanup for schema {self.schema}")

        try:
            schema = self.inventory.getSchema(self.schema)
            if schema:
                for bucket in self.inventory.nextBucket(schema):
                    print(f"Dropping bucket {bucket.name}")
                    if self.db.is_bucket(bucket.name):
                        self.db.drop_bucket(bucket.name)
        except Exception as err:
            raise TestExecError(f"cleanup failed: {err}")

    def test_init(self, bypass=False):
        collection_list = []
        rule_list = []
        tasks = set()
        end_char = '\r'
        executor = concurrent.futures.ThreadPoolExecutor()
        db_index = cb_index(self.host, self.username, self.password, self.tls, self.external_network, restore=self.session_cache, cloud=self.cloud_api)
        db_index.connect()

        cluster_memory = self.db.get_memory_quota
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
                        self.db.create_bucket(bucket.name, quota=self.bucket_memory)
                        self.db.bucket_wait(bucket.name)
                        db_index.connect_bucket(bucket.name)
                    for scope in self.inventory.nextScope(bucket):
                        if scope.name != '_default' and not bypass:
                            print("Creating scope {}".format(scope.name))
                            self.db.create_scope(scope.name)
                            self.db.scope_wait(scope.name)
                            db_index.connect_scope(scope.name)
                        elif not bypass:
                            db_index.connect_scope('_default')
                        for collection in self.inventory.nextCollection(scope):
                            if collection.name != '_default' and not bypass:
                                print("Creating collection {}".format(collection.name))
                                self.db.create_collection(collection.name)
                                self.db.collection_wait(collection.name)
                                db_index.connect_collection(collection.name)
                            elif not bypass:
                                db_index.connect_collection('_default')
                            if self.inventory.hasPrimaryIndex(collection) and not bypass:
                                print(f"Creating primary index on {collection.name} with {self.replica_count} replica(s)")
                                tasks.add(executor.submit(db_index.create_index, collection.name, replica=self.replica_count))
                            if self.inventory.hasIndexes(collection) and not bypass:
                                for index_field, index_name in self.inventory.nextIndex(collection):
                                    print(f"Creating index {index_name} on {index_field} with {self.replica_count} replica(s)")
                                    tasks.add(executor.submit(db_index.create_index, collection.name, field=index_field, index_name=index_name, replica=self.replica_count))
                            collection_list.append(collection)
                print("Waiting for index tasks to complete ... ")
                task_count = len(tasks)
                while tasks:
                    done, tasks = concurrent.futures.wait(tasks, return_when=concurrent.futures.FIRST_COMPLETED)
                    current_count = len(done)
                    progress = round((current_count / task_count) * 100)
                    print(f"     {progress}%", end=end_char)
                    for task in done:
                        try:
                            result = task.result()
                        except Exception as err:
                            raise TestExecError(f"Schema {self.schema} init error: {err}")
                sys.stdout.write("\033[K")
                print("done.")
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
        primary_key_list = []
        end_char = '\r'

        self.logger.info("run_link_rule: startup")

        if len(foreign_keyspace) != 4 and len(primary_keyspace) != 4:
            raise RulesError("runLinkRule: link rule key syntax incorrect")

        foreign_bucket, foreign_scope, foreign_collection, foreign_field = foreign_keyspace
        primary_bucket, primary_scope, primary_collection, primary_field = primary_keyspace

        if foreign_bucket != primary_bucket:
            raise RulesError("cross bucket linking is not supported")

        if foreign_scope != primary_scope:
            raise RulesError("cross scope linking is not supported")

        try:
            self.logger.debug(f"run_link_rule: connecting to bucket and collections {foreign_collection} {primary_collection}")
            self.db.bucket_s(primary_bucket)
            self.db.scope_s(primary_scope)
            self.db.collection_s(foreign_collection)
            self.db.collection_s(primary_collection)
            self.logger.debug(f"run_link_rule: bucket and collections connected")
        except Exception as err:
            raise RulesError(f"link: can not connect to database: {err}")

        print(f" [1] Building primary key list")

        result = self.db.cb_query_s(field=primary_field, name=primary_collection)

        for row in result:
            primary_key_list.append(row[primary_field])

        foreign_record_count = self.db.collection_count_s(foreign_collection)

        if foreign_record_count != len(primary_key_list):
            raise RulesError("runLinkRule: primary and foreign record counts are unequal")

        print(f" [2] Inserting keys into field {foreign_field} in {foreign_collection}")
        total_inserted = 0
        for sub_list in list(self.list_block(primary_key_list, self.default_kv_batch_size)):
            total_inserted += len(sub_list)
            progress = round((total_inserted / foreign_record_count) * 100)
            print(f"     {progress}%", end=end_char)
            self.db.cb_subdoc_multi_upsert_s(sub_list, foreign_field, sub_list, name=foreign_collection)
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
        end_char = ''
        print("Waiting for indexes to settle")
        try:
            schema = self.inventory.getSchema(self.schema)
            if schema:
                for bucket in self.inventory.nextBucket(schema):
                    self.db_index.connect_bucket(bucket.name)
                    for scope in self.inventory.nextScope(bucket):
                        self.db_index.connect_scope(scope.name)
                        for collection in self.inventory.nextCollection(scope):
                            self.db_index.connect_collection(collection.name)
                            if self.inventory.hasPrimaryIndex(collection):
                                print(f"Waiting for primary index on {collection.name} ...", end=end_char)
                                self.db_index.index_wait(name=collection.name)
                                print("done.")
                            if self.inventory.hasIndexes(collection):
                                for index_field, index_name in self.inventory.nextIndex(collection):
                                    print(f"Waiting for index {index_name} on field {index_field} in keyspace {self.db_index.db.keyspace_s(collection.name)} ...", end=end_char)
                                    self.db_index.index_wait(name=collection.name, field=index_field, index_name=index_name)
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
            self.db.cluster_health_check()
            print("done.")
        except Exception as err:
            raise TestPauseError(f"cluster health not ok: {err}")

    def calc_batch_size(self, collection, mode):
        candidate_size = None

        if mode == KV_TEST:
            try:
                buf_size = sys_info().get_net_buffer()
            except Exception:
                buf_size = None

            if buf_size:
                slice_size = buf_size
                per_thread = slice_size / (collection.size * self.run_threads)
                candidate_size = round(per_thread)

            if collection.default_batch_size and candidate_size:
                candidate_size = min(candidate_size, collection.default_batch_size)

            if self.throughput:
                total_data = collection.size * self.run_threads
                calc_batch_size = self.throughput / total_data
                new_batch_size = round(calc_batch_size * .7)
                candidate_size = min(candidate_size, new_batch_size)

            batch_size = min(candidate_size, self.default_kv_batch_size) if candidate_size else self.default_kv_batch_size
            return batch_size

        elif mode == QUERY_TEST:
            return self.default_query_batch_size

        else:
            return 1

    def test_launch_a(self, read_p=100, write_p=0, mode=KV_TEST):
        if not self.collection_list:
            raise TestRunError("test not initialized")

        run_mode = 'async' if self.aio else 'sync'
        mode_text = self.test_print_name(mode)

        print("Beginning {} {} test with {} instances.".format(run_mode, mode_text, self.run_threads))

        for coll_obj in self.collection_list:
            telemetry_queue = multiprocessing.Queue()
            count = multiprocessing.Value('i')
            run_flag = multiprocessing.Value('i')
            status_vector = multiprocessing.Array('i', [0]*10)
            count.value = 0
            run_flag.value = 1
            input_json = coll_obj.schema

            if coll_obj.record_count:
                operation_count = coll_obj.record_count
            else:
                operation_count = self.record_count

            print(f"Collection {coll_obj.name} document size: {self.format_bytes(coll_obj.size)}")
            coll_obj.batch_size = self.calc_batch_size(coll_obj, mode)
            print(f"Collection {coll_obj.name} mode {mode_text} batch size: {coll_obj.batch_size}")

            tm = test_mods(self.host, self.username, self.password, self.tls, self.external_network,
                           self.session_cache, coll_obj.batch_size, self.id_field, self.run_threads, self.thread_max)

            status_thread = multiprocessing.Process(target=tm.status_output, args=(operation_count, run_flag, telemetry_queue, status_vector))
            status_thread.daemon = True
            status_thread.start()

            print(f"Starting test on {coll_obj.name} with {operation_count:,} records - {read_p}% get, {write_p}% update")
            start_time = time.perf_counter()

            instances = []
            for n in range(self.run_threads):
                instances.append(self.loop.create_task(tm.async_test_run(mode, input_json, count, coll_obj, operation_count, telemetry_queue, write_p, n, status_vector)))
                status_vector[1] += 1

            self.loop.run_until_complete(asyncio.gather(*instances))

            run_flag.value = 0
            status_thread.join()
            end_time = time.perf_counter()

            print("Test completed in {}".format(
                time.strftime("%H hours %M minutes %S seconds.", time.gmtime(end_time - start_time))))

    def test_launch_s(self, read_p=100, write_p=0, mode=KV_TEST):
        if not self.collection_list:
            raise TestRunError("test not initialized")

        run_mode = 'async' if self.aio else 'sync'
        mode_text = self.test_print_name(mode)

        print("Beginning {} {} test with {} instances.".format(run_mode, mode_text, self.run_threads))

        for coll_obj in self.collection_list:
            telemetry_queue = multiprocessing.Queue()
            count = multiprocessing.Value('i')
            run_flag = multiprocessing.Value('i')
            status_vector = multiprocessing.Array('i', [0]*10)
            count.value = 0
            run_flag.value = 1
            input_json = coll_obj.schema

            if coll_obj.record_count:
                operation_count = coll_obj.record_count
            else:
                operation_count = self.record_count

            print(f"Collection {coll_obj.name} document size: {self.format_bytes(coll_obj.size)}")
            coll_obj.batch_size = self.calc_batch_size(coll_obj, mode)
            print(f"Collection {coll_obj.name} mode {mode_text} batch size: {coll_obj.batch_size}")

            tm = test_mods(self.host, self.username, self.password, self.tls, self.external_network,
                           self.session_cache, coll_obj.batch_size, self.id_field, self.run_threads, self.thread_max)

            status_thread = multiprocessing.Process(target=tm.status_output, args=(operation_count, run_flag, telemetry_queue, status_vector))
            status_thread.daemon = True
            status_thread.start()

            print(f"Starting test on {coll_obj.name} with {operation_count:,} records - {read_p}% get, {write_p}% update")
            start_time = time.perf_counter()

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
                    target=tm.sync_test_run,
                    args=(mode, input_json, count, coll_obj, operation_count,
                          telemetry_queue, write_p, n, status_vector)))
                instances[n].daemon = True
                instances[n].start()
                status_vector[1] += 1
                n += 1

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
        mask = mode | RANDOM_KEYS
        mode_text = self.test_print_name(mode)

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

            print(f"Collection {coll_obj.name} document size: {self.format_bytes(coll_obj.size)}")
            coll_obj.batch_size = self.calc_batch_size(coll_obj, mode)
            print(f"Collection {coll_obj.name} mode {mode_text} batch size: {coll_obj.batch_size}")

            tm = test_mods(self.host, self.username, self.password, self.tls, self.external_network,
                           self.session_cache, coll_obj.batch_size, self.id_field, self.run_threads, self.thread_max)

            status_thread = multiprocessing.Process(target=tm.status_output, args=(operation_count, run_flag, telemetry_queue, status_vector))
            status_thread.daemon = True
            status_thread.start()

            print(f"Starting ramp test - {read_p}% get, {write_p}% update")

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
                        target=tm.sync_test_run,
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
