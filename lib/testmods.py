##
##

from lib.cbutil.cbdebug import cb_debug
from lib.cbutil.randomize import randomize, fastRandom
from lib.constants import *
from lib.cbutil.cbconnect import cb_connect
from queue import Empty
import asyncio
import time
import numpy as np
import sys
import threading
import concurrent.futures


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


class test_mods(object):

    def __init__(self,  hostname: str, username: str, password: str, ssl, external, restore, batch_size, id_field, run_t, max_t):
        self.host = hostname
        self.username = username
        self.password = password
        self.tls = ssl
        self.external_network = external
        self.session_cache = restore
        self.batch_size = batch_size
        self.id_field = id_field
        self.run_threads = run_t
        self.thread_max = max_t
        debugger = cb_debug(self.__class__.__name__, overwrite=True)
        self.logger = debugger.logger

    def test_mask(self, bits):
        if bits & KV_TEST:
            return KV_TEST
        elif bits & QUERY_TEST:
            return QUERY_TEST
        elif bits & REMOVE_DATA:
            return REMOVE_DATA
        else:
            return 0x0

    def is_random_mask(self, bits):
        if bits & RANDOM_KEYS:
            return True
        else:
            return False

    def mod_unhandled_exception(self, loop, context):
        err = context.get("exception", context['message'])
        if isinstance(err, Exception):
            self.logger.error(f"unhandled exception: type: {err.__class__.__name__} msg: {err} cause: {err.__cause__}")
        else:
            self.logger.error(f"unhandled error: {err}")

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

    def test_run_a(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(self.mod_unhandled_exception)
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

        logger.info(f"beginning async test instance {n}")

        mode = self.test_mask(mask)
        is_random = self.is_random_mask(mask)

        run_batch_size = self.batch_size

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
                        if mode == REMOVE_DATA:
                            tasks.append(asyncio.create_task(db.cb_remove_a(record_number, name=coll_obj.name)))
                        elif mode == QUERY_TEST:
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
        debugger = cb_debug('sync_test_run')
        logger = debugger.logger
        begin_time = time.time()

        logger.info(f"beginning test instance {n}")

        mode = self.test_mask(mask)
        is_random = self.is_random_mask(mask)

        run_batch_size = self.batch_size
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=run_batch_size)

        if status_vector[0] == 1:
            logger.info(f"test_thread_{n:03d}: aborting startup due to stop signal")
            return

        try:
            logger.info(f"test_thread_{n:03d}: connecting to {self.host}")
            db = cb_connect(self.host, self.username, self.password, self.tls, self.external_network, restore=self.session_cache)
            db.quick_connect_s(coll_obj.bucket, coll_obj.scope, coll_obj.name)
        except Exception as err:
            status_vector[0] = 1
            status_vector[2] += 1
            logger.info(f"test_thread_{n:03d}: db connection error: {err}")
            return

        try:
            r = randomize()
            r.prepareTemplate(input_json)
        except Exception as err:
            status_vector[0] = 1
            status_vector[2] += 1
            logger.info(f"test_thread_{n:03d}: randomizer error: {err}")
            return

        if status_vector[0] == 1:
            logger.info(f"test_thread_{n:03d}: aborting run due to stop signal")
            return

        status_vector[3] += 1
        logger.info(f"test_thread_{n:03d}: commencing run")
        while True:
            try:
                tasks = set()
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
                        tasks.add(executor.submit(db.cb_upsert_s, record_number, document, name=coll_obj.name))
                    else:
                        if mode == REMOVE_DATA:
                            tasks.add(executor.submit(db.cb_remove_s, record_number, name=coll_obj.name))
                        elif mode == QUERY_TEST:
                            tasks.add(executor.submit(db.cb_query_s, field=query_field, name=coll_obj.name, where=id_field, value=record_number))
                        else:
                            tasks.add(executor.submit(db.cb_get_s, record_number, name=coll_obj.name))
            except Exception as err:
                status_vector[0] = 1
                status_vector[2] += 1
                logger.error(f"test_thread_{n:03d}: task error #{status_vector[2]}: {err}")
            if len(tasks) > 0:
                task_count = len(tasks)
                while tasks:
                    done, tasks = concurrent.futures.wait(tasks, return_when=concurrent.futures.FIRST_COMPLETED)
                    for task in done:
                        try:
                            result = task.result()
                        except Exception as err:
                            logger.error(f"test_thread_{n:03d}: task error: {err}")
                end_time = time.time()
                loop_total_time = end_time - begin_time
                telemetry[0] = n
                telemetry[1] = task_count
                telemetry[2] = loop_total_time
                telemetry_packet = ':'.join(str(i) for i in telemetry)
                telemetry_queue.put(telemetry_packet)
                if loop_total_time >= time_threshold:
                    status_vector[0] = 1
                    logger.error(f"test_thread_{n:03d}: max latency exceeded")
            else:
                break
