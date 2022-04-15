##
##

import multiprocessing
from lib.cbutil.cbconnect import cb_connect
from lib.cbutil.cbindex import cb_index
from lib.cbutil.randomize import randomize, fastRandom
from lib.inventorymgr import inventoryManager
from lib.cbutil.exceptions import *
from lib.exceptions import *
import json
import os
import numpy as np
import asyncio
import time

KV_TEST = 0x0001
QUERY_TEST = 0x0002
REMOVE_DATA = 0x0003


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
        self.run_threads = os.cpu_count() * 5
        self.replica_count = 1
        self.rule_list = None
        self.collection_list = None
        self.rules_run = False
        self.test_playbook = "default"
        self.read_config_file(config_file)
        self.read_schema_file(schema_file)
        self.record_count = self.default_operation_count
        self.operation_count = self.default_operation_count
        self.bucket_memory = None
        self.id_field = self.default_id_field

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
            self.default_bucket_memory = self.settings['bucket_memory']
        except KeyError:
            raise ConfigFileError("can not read default settings")

    def read_schema_file(self, filepath):
        if not os.path.exists(filepath):
            raise SchemaFileError("can not locate schema file {}".format(filepath))

        try:
            with open(filepath, 'r') as schemafile:
                inventory_data = json.load(schemafile)
                self.inventory = inventoryManager(inventory_data)
                schemafile.close()
        except KeyError:
            raise SchemaFileError("can not read schema file {}".format(filepath))
        except Exception as err:
            raise SchemaFileError("can not open schema file {}: {}".format(filepath, err))

    def get_next_task(self):
        try:
            for item in self.playbooks[self.test_playbook]:
                yield item, self.playbooks[self.test_playbook][item]['test']
        except KeyError:
            raise TestConfigError("test scenario syntax error")


class print_host_map(cbPerfBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self):
        db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network)
        db.print_host_map()


class test_exec(cbPerfBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.parameters.count:
            self.record_count = self.parameters.count
        if self.parameters.ops:
            self.operation_count = self.parameters.ops
        if self.parameters.trun:
            self.run_threads = self.parameters.trun
        if self.parameters.memquota:
            self.bucket_memory = self.parameters.memquota
        if self.parameters.file:
            self.input_file = self.parameters.file
        if self.parameters.id:
            self.id_field = self.parameters.id
        if self.parameters.query:
            self.query_field = self.parameters.query
        if self.parameters.debug:
            self.debug = self.parameters.debug
        # if self.parameters.dryrun:
        #     self.dryRunFlag = self.parameters.dryrun
        # if self.parameters.model:
        #     self.runCpuModelFlag = self.parameters.model
        if self.parameters.sync:
            self.aio = False
            self.batchSize = 1
        # if self.parameters.skipbucket:
        #     self.skipBucket = self.parameters.skipbucket
        # if self.parameters.clean:
        #     self.runRemoveTest = self.parameters.clean

        if self.input_file:
            self.schema = "external_file"
        else:
            self.schema = self.parameters.schema

        if self.operation_count > self.record_count:
            raise ParameterError("Error: Operation count must be equal or less than record count.")

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

    def reset_counter(self, i=1):
        with self.count.get_lock():
            self.count.value = i

    @property
    def next_counter(self):
        with self.count.get_lock():
            current = self.count.value
            self.count.value += 1
        return current

    def run(self):
        print("start")
        for index, element in enumerate(self.get_next_task()):
            print(element[0])
            print(element[1])

        self.test_init(bypass=True)
        self.test_launch()

    def test_init(self, bypass=False):
        collection_list = []
        rule_list = []

        db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network)
        db.connect_s()
        db_index = cb_index(self.host, self.username, self.password, self.tls, self.external_network)
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
                        db_index.connect_bucket(bucket.name)
                    for scope in self.inventory.nextScope(bucket):
                        if scope.name != '_default' and not bypass:
                            print("Creating scope {}".format(scope.name))
                            db.create_scope(scope.name)
                            db_index.connect_scope(scope.name)
                        for collection in self.inventory.nextCollection(scope):
                            if collection.name != '_default' and not bypass:
                                print("Creating collection {}".format(collection.name))
                                db.create_collection(collection.name)
                                db_index.connect_collection(collection.name)
                            if self.inventory.hasPrimaryIndex(collection) and not bypass:
                                db_index.create_index(collection.name, replica=self.replica_count)
                            if self.inventory.hasIndexes(collection) and not bypass:
                                for index_field, index_name in self.inventory.nextIndex(collection):
                                    print("Creating index {}".format(index_name))
                                    db_index.create_index(collection.name, field=index_field,
                                                          replica=self.replica_count)
                            collection_list.append(collection)
                if self.inventory.hasRules(schema):
                    for rule in self.inventory.nextRule(schema):
                        rule_list.append(rule)
                self.rule_list = rule_list
                self.collection_list = collection_list
            else:
                raise ParameterError("Schema {} not found".format(self.schema))
        except Exception as err:
            raise TestExecError("Initialization failed: {}".format(err))

    def run_link_rule(self, foreign_keyspace, primary_keyspace):
        loop = asyncio.get_event_loop()
        primary_key_list = []

        db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network)
        db.connect_s()

        if len(foreign_keyspace) != 4 and len(primary_keyspace) != 4:
            raise RulesError("runLinkRule: link rule key syntax incorrect")

        foreign_bucket, foreign_scope, foreign_collection, foreign_field = foreign_keyspace
        primary_bucket, primary_scope, primary_collection, primary_field = primary_keyspace

        if foreign_bucket != primary_bucket:
            raise RulesError("cross bucket linking is not supported")

        if foreign_scope != primary_scope:
            raise RulesError("cross scope linking is not supported")

        db.bucket_s(primary_bucket)
        db.scope_s(primary_scope)
        db.collection_s(foreign_collection)
        db.collection_s(primary_collection)

        if self.aio:
            result = loop.run_until_complete(db.cb_query_a(field=primary_field, name=primary_collection))
        else:
            result = db.cb_query_s(field=primary_field, name=primary_collection)

        for row in result:
            primary_key_list.append(row[primary_field])

        foreign_record_count = db.collection_count(foreign_collection)

        if foreign_record_count != len(primary_key_list):
            raise RulesError("runLinkRule: primary and foreign record counts are unequal")

        for index, key in enumerate(primary_key_list):
            if self.aio:
                insert_value = loop.run_until_complete(db.cb_subdoc_upsert_a(key, foreign_field, key,
                                                                             name=foreign_collection))
            else:
                insert_value = db.cb_subdoc_upsert_s(key, foreign_field, key, name=foreign_collection)

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

    def status_output(self, total_count, run_flag, count, ops_run, ops_time, total_ops):
        sample_count = 1
        total_tps = 0
        total_time = 0
        max_tps = 0
        max_time = 0
        avg_tps = 0
        avg_time = 0
        percentage = 0
        time_delta = 0
        end_char = '\r'

        while run_flag.value == 1:
            ops_vector = np.array(ops_run)
            time_vector = np.array(ops_time)
            ops_average = ops_vector[np.nonzero(ops_vector)].mean()
            time_average = time_vector[np.nonzero(time_vector)].mean()
            trans_per_sec = np.rint(ops_average / time_average)
            total_ops = np.sum(total_ops)
            if total_ops > 0:
                time_delta = time_average / ops_average
                total_tps = total_tps + trans_per_sec
                total_time = total_time + time_delta
                avg_tps = total_tps / sample_count
                avg_time = total_time / sample_count
                sample_count += 1
            if trans_per_sec > max_tps:
                max_tps = trans_per_sec
            if time_delta > max_time:
                max_time = time_delta
            if total_ops > 0:
                percentage = round((total_count / total_ops) * 100)
            print(f"=> {total_ops} of {total_count}, {time_delta:.6f} time, {trans_per_sec} TPS, {percentage}%%",
                  end=end_char)

        sys.stdout.write("\033[K")
        print(f"=> {total_count} of {total_count}, {percentage}%%")
        print("Test Done.")
        print("{} average TPS.".format(round(avg_tps)))
        print("{} maximum TPS.".format(round(max_tps)))
        print("{:.6f} average time.".format(avg_time))
        print("{:.6f} maximum time.".format(max_time))

    def test_launch(self, read_p=0, write_p=100, mode=KV_TEST):
        if not self.collection_list:
            raise TestRunError("test not initialized")

        print("Beginning test with {} instances.".format(self.run_threads))

        for coll_obj in self.collection_list:
            count = multiprocessing.Value('i')
            run_flag = multiprocessing.Value('i')
            ops_run = multiprocessing.Array('i', self.run_threads)
            ops_time = multiprocessing.Array('f', self.run_threads)
            total_ops = multiprocessing.Array('i', self.run_threads)
            count.value = 0
            run_flag.value = 1
            input_json = coll_obj.schema

            if coll_obj.record_count:
                operation_count = coll_obj.record_count
            else:
                operation_count = self.record_count

            status_thread = multiprocessing.Process(
                target=self.status_output, args=(operation_count, run_flag, count, ops_run, ops_time, total_ops))
            status_thread.daemon = True
            status_thread.start()

            print(f"Starting test with {operation_count:,} records - {read_p}%% get, {write_p}%% update")
            start_time = time.perf_counter()

            instances = [
                multiprocessing.Process(
                    target=self.test_run,
                    args=(mode, input_json, count, coll_obj, operation_count,
                          ops_run, ops_time, total_ops, read_p, write_p, n)) for n in range(self.run_threads)
            ]
            for p in instances:
                p.daemon = True
                p.start()

            for p in instances:
                p.join()

            run_flag.value = 0
            status_thread.join()
            end_time = time.perf_counter()

            # for i in range(len(total_ops)):
            #     print("{}".format(total_ops[1]))
            print("Test completed in {}".format(
                time.strftime("%H hours %M minutes %S seconds.", time.gmtime(end_time - start_time))))

    def test_run(self, mode, input_json, count, coll_obj, record_count,
                 ops_run, ops_time, total_ops, read_p, write_p, n):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        tasks = []
        rand_gen = fastRandom(record_count)
        id_field = coll_obj.id
        query_field = next((i['field'] for i in coll_obj.indexes if i['field'] != id_field), id_field)
        is_random = False
        op_select = rwMixer(write_p)

        if mode == QUERY_TEST:
            run_batch_size = self.default_query_batch_size
        else:
            run_batch_size = self.default_kv_batch_size

        db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network)
        db.connect_s()
        db.bucket_s(coll_obj.bucket)
        db.scope_s(coll_obj.scope)
        db.collection_s(coll_obj.name)

        try:
            r = randomize()
            r.prepareTemplate(input_json)
        except Exception as err:
            raise TestRunError("Can not load JSON template: {}".format(err))

        while True:
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
                    if self.aio:
                        tasks.append(db.cb_upsert_a(record_number, document, name=coll_obj.name))
                    else:
                        result = db.cb_upsert_s(record_number, document, name=coll_obj.name)
                        tasks.append(result)
                else:
                    if mode == REMOVE_DATA:
                        if self.aio:
                            tasks.append(db.cb_remove_a(record_number, name=coll_obj.name))
                        else:
                            result = db.cb_remove_s(record_number, name=coll_obj.name)
                            tasks.append(result)
                    elif mode == QUERY_TEST:
                        if self.aio:
                            tasks.append(
                                db.cb_query_a(field=query_field, name=coll_obj.name, where=id_field,
                                              value=record_number))
                        else:
                            result = db.cb_query_s(field=query_field, name=coll_obj.name, where=id_field,
                                                   value=record_number)
                            tasks.append(result)
                    else:
                        if self.aio:
                            tasks.append(db.cb_get_a(record_number, name=coll_obj.name))
                        else:
                            result = db.cb_get_s(record_number, name=coll_obj.name)
                            tasks.append(result)
            if len(tasks) > 0:
                if self.aio:
                    try:
                        result = loop.run_until_complete(asyncio.gather(*tasks))
                    except Exception as err:
                        raise TestRunError("run error: {}".format(err))
                end_time = time.time()
                loop_total_time = end_time - begin_time
                ops_run[n] = len(tasks)
                total_ops[n] += len(tasks)
                ops_time[n] = loop_total_time
            else:
                break
