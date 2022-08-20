#!/usr/bin/env python3

import os
import sys

import couchbase.result

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from lib import system
from lib.cbutil import cbconnect
from lib.cbutil import cbindex
from lib.cbutil.randomize import randomize
from lib.executive import print_host_map, test_exec, schema_admin
import argparse
import asyncio
import re
import types
import datetime
import time
import sys
from shutil import copyfile
import subprocess
import warnings
import traceback

document = {
    "id": 1,
    "data": "data",
    "one": "one",
    "two": "two",
    "three": "tree"
}
new_document = {
    "id": 1,
    "data": "new",
    "one": "one",
    "two": "two",
    "three": "tree"
}
query_result = [
    {
        'data': 'data'
    }
]
failed = 0
tests_run = 0
replica_count = 1
VERSION = "1.0"
warnings.filterwarnings("ignore")

class CheckCompare(object):

    def __init__(self):
        self.num = False
        self.chr = False
        self.bln = False
        self.tim = False
        self.low = 0
        self.high = 0
        self.chars = 0
        self.p = None
        self.bv = False

    def num_range(self, l: int, h: int):
        self.num = True
        self.chr = False
        self.bln = False
        self.tim = False
        self.low = l
        self.high = h

    def pattern(self, s):
        self.num = False
        self.chr = True
        self.bln = False
        self.tim = False
        self.p = re.compile(s)

    def boolean(self):
        self.num = False
        self.chr = False
        self.bln = True
        self.tim = False

    def time(self):
        self.num = False
        self.chr = False
        self.bln = False
        self.tim = True

    def check(self, v):
        if isinstance(v, types.GeneratorType):
            v = next(v)
        if self.num:
            if self.low <= int(v) <= self.high:
                return True
            else:
                return False
        elif self.chr:
            if self.p.match(v):
                return True
            else:
                return False
        elif self.bln:
            if type(v) == bool:
                return True
            else:
                return False
        elif self.tim:
            if isinstance(v, datetime.datetime):
                return True
            else:
                return False


def check_host_map():
    out_file = open("test_output.out", "r")
    line = out_file.readline()
    p = re.compile("^.*Cluster Host List.*$")
    if not p.match(line):
        return False
    p = re.compile("^ \[[0-9]+\] .*$")
    for line in out_file.readlines():
        if not p.match(line):
            print(f"Unexpected output: {line}")
            return False
    return True


def check_clean():
    out_file = open("test_output.out", "r")
    p = re.compile("^Dropping bucket .*$")
    for line in out_file.readlines():
        if p.match(line):
            return True
    return False


def check_status_output():
    matches_found = 0
    out_file = open("test_output.out", "r")
    for line in out_file.readlines():
        p = re.compile("^Creating bucket .*$")
        if p.match(line):
            matches_found += 1
            continue
        p = re.compile("^Creating index [a-z_]+ on [a-z_]+ with [0-9]+ replica.*$")
        if p.match(line):
            matches_found += 1
            continue
        p = re.compile("^0 errors.*$")
        if p.match(line):
            matches_found += 1
            continue
        p = re.compile("^.* [0-9]+ of [0-9]+, 100%.*$")
        if p.match(line):
            matches_found += 1
            continue
    if matches_found >= 4:
        return True
    else:
        return False


def check_run_output():
    matches_found = 0
    out_file = open("test_output.out", "r")
    line = out_file.readline()
    while line:
        p = re.compile("^Beginning [a]*sync .* test with [0-9]+ instances.*$")
        if p.match(line):
            test_line = out_file.readline()
            while test_line:
                p = re.compile("^.* [0-9]+ of [0-9]+, 100%.*$")
                if p.match(test_line):
                    matches_found += 1
                    break
                test_line = out_file.readline()
        line = out_file.readline()
    if matches_found == 7:
        return True
    else:
        return False


def check_ramp_output():
    load_found = 0
    tests_found = 0
    out_file = open("test_output.out", "r")
    line = out_file.readline()
    while line:
        p = re.compile("^Beginning [a]*sync .* test with [0-9]+ instances.*$")
        if p.match(line):
            test_line = out_file.readline()
            while test_line:
                p = re.compile("^.* [0-9]+ of [0-9]+, 100%.*$")
                if p.match(test_line):
                    load_found += 1
                    break
                test_line = out_file.readline()
        p = re.compile("^Beginning [a]*sync test ramp with max [0-9]+ instances.*$")
        if p.match(line):
            test_line = out_file.readline()
            while test_line:
                p = re.compile("^0 errors.*$")
                if p.match(test_line):
                    tests_found += 1
                    break
                test_line = out_file.readline()
        line = out_file.readline()
    if load_found == 1 and tests_found == 2:
        return True
    else:
        return False


def check_list(a, b):
    for item in a:
        if item not in b:
            return False
    return True


def truncate_output_file():
    file = open("test_output.out", "w")
    file.close()


def test_unhandled_exception(loop, context):
    err = context.get("exception", context['message'])
    if isinstance(err, Exception):
        print(f"unhandled exception: type: {err.__class__.__name__} msg: {err} cause: {err.__cause__}")
    else:
        print(f"unhandled error: {err}")


def test_step(check, fun, *args, __name=None, **kwargs):
    global failed, tests_run
    result = None
    fun_name = ""
    try:
        tests_run += 1

        if __name:
            fun_name = __name
        else:
            fun_name = fun.__name__

        print(f" {tests_run}) Test {fun_name}() ... ", end='')
        sys.stdout.flush()

        stdout = sys.stdout
        sys.stdout = open("test_output.out", "a")
        if __name:
            result = fun
        else:
            result = fun(*args, **kwargs)
        sys.stdout = stdout

        if check:
            if type(check) == list:
                assert check_list(check, result) is True
            elif type(check) == bool and check is True:
                assert result is not None
            elif type(result) == couchbase.result.GetResult:
                assert check == result.content_as[dict]
            elif type(check) == CheckCompare:
                assert check.check(result)
            elif callable(check):
                assert check() is True
            else:
                assert check == result

        print("Ok")
        return result
    except Exception as err:
        tb = traceback.format_exc()
        sys.stdout.flush()
        with open("test_output.out", "a") as out_file:
            out_file.write(f"Type of check value = {type(check)}\n")
            out_file.write(f"Check = {check}\n")
            out_file.write(f"Result = {result}\n")
            out_file.write(tb)
            out_file.write("\n")
            out_file.close()
        copyfile("test_output.out", f"test_fail_{fun_name}.out")
        copyfile("cb_debug.log", f"test_fail_{fun_name}.log")
        print(f"Step failed: function {fun_name}: {err}")
        failed += 1


async def async_test_step(check, fun, *args, **kwargs):
    global failed, tests_run
    result = None
    fun_name = fun.__name__
    try:
        tests_run += 1

        print(f" {tests_run}) Test {fun_name}() ... ", end='')

        stdout = sys.stdout
        sys.stdout = open("test_output.out", "a")
        result = await fun(*args, **kwargs)
        sys.stdout = stdout

        if check:
            if type(check) == list:
                assert check_list(check, result) is True
            elif type(check) == bool and check is True:
                assert result is not None
            elif type(result) == couchbase.result.GetResult:
                assert check == result.content_as[dict]
            elif type(check) == CheckCompare:
                assert check.check(result)
            elif callable(check):
                assert check() is True
            else:
                assert check == result

        print("Ok")
    except Exception as err:
        tb = traceback.format_exc()
        sys.stdout.flush()
        with open("test_output.out", "a") as out_file:
            out_file.write(f"Type of check value = {type(check)}\n")
            out_file.write(f"Check = {check}\n")
            out_file.write(f"Result = {result}\n")
            out_file.write(tb)
            out_file.write("\n")
            out_file.close()
        copyfile("test_output.out", f"test_fail_{fun_name}.out")
        copyfile("cb_debug.log", f"test_fail_{fun_name}.log")
        print(f"Step failed: function {fun_name}: {err}")
        failed += 1


def cb_sync_test_set(host, username, password, bucket, scope, collection, tls, external_network, cloud_api):
    global replica_count
    db = cbconnect.cb_connect(host, username, password, ssl=tls, external=external_network, cloud=cloud_api)
    db_index = cbindex.cb_index(host, username, password, ssl=tls, external=external_network, cloud=cloud_api)

    test_step(None, db.connect_s)
    test_step(None, db_index.connect)
    test_step(None, db.create_bucket, bucket)
    test_step(None, db.bucket_wait, bucket)
    if scope == '_default':
        test_step(None, db.scope_s, scope)
    else:
        test_step(None, db.create_scope, scope)
        test_step(None, db.scope_wait, scope)
    if collection == '_default':
        test_step(None, db.collection_s, collection)
    else:
        test_step(None, db.create_collection, collection)
        test_step(None, db.collection_wait, collection)
    test_step(True, db.is_bucket, bucket)
    test_step(True, db.is_scope, scope)
    test_step(True, db.is_collection, collection)
    test_step(None, db_index.connect_bucket, bucket)
    test_step(None, db_index.connect_scope, scope)
    test_step(None, db_index.connect_collection, collection)
    test_step(None, db_index.create_index, collection, replica=replica_count)
    test_step(None, db_index.create_index, collection, field="data", index_name="data_index", replica=replica_count)
    test_step(None, db_index.index_wait, collection)
    test_step(None, db_index.index_wait, collection, field="data", index_name="data_index")
    test_step(None, db.cb_upsert_s, "test::1", document, name=collection)
    test_step(document, db.cb_get_s, "test::1", name=collection)
    test_step(1, db.collection_count_s, collection, expect=1)
    test_step(query_result, db.cb_query_s, field="data", name=collection, empty_retry=True)
    test_step(None, db.cb_upsert_s, "test::2", document, name=collection)
    test_step(None, db.cb_subdoc_multi_upsert_s, ["test::1", "test::2"], "data", ["new", "new"], name=collection)
    test_step(new_document, db.cb_get_s, "test::1", name=collection)
    test_step(2, db.collection_count_s, collection, expect=2)
    test_step(None, db.cb_upsert_s, "test::3", document, name=collection)
    test_step(None, db.cb_subdoc_upsert_s, "test::3", "data", "new", name=collection)
    test_step(new_document, db.cb_get_s, "test::3", name=collection)
    test_step(None, db.drop_bucket, bucket)


def cb_async_test_set(host, username, password, bucket, scope, collection, tls, external_network, cloud_api):
    global replica_count
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(test_unhandled_exception)
    db = cbconnect.cb_connect(host, username, password, ssl=tls, external=external_network, cloud=cloud_api)
    db_index = cbindex.cb_index(host, username, password, ssl=tls, external=external_network, cloud=cloud_api)

    test_step(None, db.connect_s)
    loop.run_until_complete(async_test_step(None, db.connect_a))
    test_step(None, db_index.connect)
    test_step(None, db.create_bucket, bucket)
    test_step(None, db.bucket_wait, bucket)
    loop.run_until_complete(async_test_step(None, db.bucket_a, bucket))
    if scope == '_default':
        loop.run_until_complete(async_test_step(None, db.scope_a, scope))
    else:
        test_step(None, db.create_scope, scope)
        test_step(None, db.scope_wait, scope)
        loop.run_until_complete(async_test_step(None, db.scope_a, scope))
    if collection == '_default':
        loop.run_until_complete(async_test_step(None, db.collection_a, collection))
    else:
        test_step(None, db.create_collection, collection)
        test_step(None, db.collection_wait, collection)
        loop.run_until_complete(async_test_step(None, db.collection_a, collection))
    test_step(True, db.is_bucket, bucket)
    test_step(True, db.is_scope, scope)
    test_step(True, db.is_collection, collection)
    test_step(None, db_index.connect_bucket, bucket)
    test_step(None, db_index.connect_scope, scope)
    test_step(None, db_index.connect_collection, collection)
    test_step(None, db_index.create_index, collection, replica=replica_count)
    test_step(None, db_index.create_index, collection, field="data", index_name="data_index", replica=replica_count)
    test_step(None, db_index.index_wait, collection)
    test_step(None, db_index.index_wait, collection, field="data", index_name="data_index")
    loop.run_until_complete(async_test_step(None, db.cb_upsert_a, "test::1", document, name=collection))
    loop.run_until_complete(async_test_step(document, db.cb_get_a, "test::1", name=collection))
    loop.run_until_complete(async_test_step(1, db.collection_count_a, collection, expect=1))
    loop.run_until_complete(async_test_step(query_result, db.cb_query_a, field="data", name=collection, empty_retry=True))
    loop.run_until_complete(async_test_step(None, db.cb_upsert_a, "test::2", document, name=collection))
    loop.run_until_complete(async_test_step(None, db.cb_subdoc_multi_upsert_a, ["test::1", "test::2"], "data", ["new", "new"], name=collection))
    loop.run_until_complete(async_test_step(new_document, db.cb_get_a, "test::1", name=collection))
    loop.run_until_complete(async_test_step(2, db.collection_count_a, collection, expect=2))
    loop.run_until_complete(async_test_step(None, db.cb_upsert_a, "test::3", document, name=collection))
    loop.run_until_complete(async_test_step(None, db.cb_subdoc_upsert_a, "test::3", "data", "new", name=collection))
    loop.run_until_complete(async_test_step(new_document, db.cb_get_a, "test::3", name=collection))
    test_step(None, db.drop_bucket, bucket)


def cb_connect_test(host, username, password, bucket, tls, external_network, cloud_api):
    print("Sync - Default scope and collection tests")
    cb_sync_test_set(host, username, password, bucket, '_default', '_default', tls, external_network, cloud_api)
    print("Sync - Named scope and collection tests")
    cb_sync_test_set(host, username, password, bucket, 'testscope', 'testcollection', tls, external_network, cloud_api)
    print("Async - Default scope and collection tests")
    cb_async_test_set(host, username, password, bucket, '_default', '_default', tls, external_network, cloud_api)
    print("Async - Named scope and collection tests")
    cb_async_test_set(host, username, password, bucket, 'testscope', 'testcollection', tls, external_network, cloud_api)


def randomize_test():
    r = randomize()
    c = CheckCompare()
    c.num_range(0, 9)
    test_step(c, r._randomNumber, 1)
    c.pattern('^[a-z]+$')
    test_step(c, r._randomStringLower, 8)
    c.pattern('^[A-Z]+$')
    test_step(c, r._randomStringUpper, 8)
    c.pattern('^[a-zA-Z0-9]+$')
    test_step(c, r._randomHash, 8)
    c.num_range(0, 255)
    test_step(c, r._randomBits, 8)
    c.num_range(1, 12)
    test_step(c, r._monthNumber)
    c.num_range(1, 31)
    test_step(c, r._monthDay)
    c.num_range(1920, 2022)
    test_step(c, r._yearNumber)
    c.pattern('^[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]$')
    test_step(c, r.creditCard, __name='creditCard')
    c.pattern('^[0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]$')
    test_step(c, r.socialSecurityNumber, __name='socialSecurityNumber')
    c.pattern('^[0-9][0-9][0-9]$')
    test_step(c, r.threeDigits, __name='threeDigits')
    c.pattern('^[0-9][0-9][0-9][0-9]$')
    test_step(c, r.fourDigits, __name='fourDigits')
    c.pattern('^[0-9][0-9][0-9][0-9][0-9]$')
    test_step(c, r.zipCode, __name='zipCode')
    c.pattern('^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$')
    test_step(c, r.accountNumner, __name='accountNumner')
    c.pattern('^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$')
    test_step(c, r.numericSequence, __name='numericSequence')
    c.pattern('^[0-9]+\.[0-9]+$')
    test_step(c, r.dollarAmount, __name='dollarAmount')
    c.boolean()
    test_step(c, r.booleanValue, __name='booleanValue')
    c.num_range(1920, 2022)
    test_step(c, r.yearValue, __name='yearValue')
    c.num_range(1, 12)
    test_step(c, r.monthValue, __name='monthValue')
    c.num_range(1, 31)
    test_step(c, r.dayValue, __name='dayValue')
    c.time()
    test_step(c, r.pastDate, __name='pastDate')
    c.time()
    test_step(c, r.dobDate, __name='dobDate')
    c.pattern('^[0-9]+/[0-9]+/[0-9]+$')
    past_date = r.pastDate
    test_step(c, r.pastDateSlash, past_date)
    c.pattern('^[0-9]+-[0-9]+-[0-9]+$')
    past_date = r.pastDate
    test_step(c, r.pastDateHyphen, past_date)
    c.pattern('^[a-zA-Z]+ [0-9]+ [0-9]+$')
    past_date = r.pastDate
    test_step(c, r.pastDateText, past_date)
    c.pattern('^[0-9]+/[0-9]+/[0-9]+$')
    past_date = r.dobDate
    test_step(c, r.dobSlash, past_date)
    c.pattern('^[0-9]+-[0-9]+-[0-9]+$')
    past_date = r.dobDate
    test_step(c, r.dobHyphen, past_date)
    c.pattern('^[a-zA-Z]+ [0-9]+ [0-9]+$')
    past_date = r.dobDate
    test_step(c, r.dobText, past_date)
    c.pattern('^[a-zA-Z0-9]+$')
    test_step(c, r.hashCode, __name='hashCode')
    c.pattern('^[a-zA-Z]+$')
    test_step(c, r.firstName, __name='firstName')
    c.pattern('^[a-zA-Z]+$')
    test_step(c, r.lastName, __name='lastName')
    c.pattern('^[a-zA-Z]+$')
    test_step(c, r.streetType, __name='streetType')
    c.pattern('^[a-zA-Z0-9]+$')
    test_step(c, r.streetName, __name='streetName')
    c.pattern('^[0-9]+ [a-zA-Z0-9]+ [a-zA-Z]+$')
    test_step(c, r.addressLine, __name='addressLine')
    c.pattern('^[a-zA-Z ]+$')
    test_step(c, r.cityName, __name='cityName')
    c.pattern('^[A-Z]+$')
    test_step(c, r.stateName, __name='stateName')
    c.pattern('^[0-9][0-9][0-9]-555-[0-9][0-9][0-9][0-9]$')
    test_step(c, r.phoneNumber, __name='phoneNumber')
    c.pattern('^[0-9]+-[0-9]+-[0-9]+ [0-9]+:[0-9]+:[0-9]+$')
    test_step(c, r.dateCode, __name='dateCode')
    c.pattern('^[a-z]+$')
    first = r.firstName
    last = r.lastName
    test_step(c, r.nickName, first, last)
    c.pattern('^[a-z]+\.[a-z]+@[a-z]+\.[a-z]+$')
    first = r.firstName
    last = r.lastName
    test_step(c, r.emailAddress, first, last)
    c.pattern('^[a-z]+[0-9]+$')
    first = r.firstName
    last = r.lastName
    test_step(c, r.userName, first, last)
    test_step(None, r.randImage)


def test_main(parameters, sync=False, schema="default"):
    username = parameters.user
    password = parameters.password
    hostname = parameters.host
    bucket = parameters.bucket
    external = parameters.external
    cloud_api = parameters.noapi

    truncate_output_file()
    task = print_host_map(parameters)
    test_step(check_host_map, task.run)
    parameters.command = 'load'
    parameters.count = 100
    parameters.ops = 100
    parameters.replica = 0
    parameters.threads = os.cpu_count()
    parameters.max = os.cpu_count()
    parameters.sync = sync
    parameters.schema = schema
    parameters.output = "test_output.out"
    truncate_output_file()
    task = test_exec(parameters)
    test_step(check_status_output, task.run)
    parameters.command = 'run'
    parameters.ramp = False
    truncate_output_file()
    task = test_exec(parameters)
    test_step(check_run_output, task.run)
    parameters.ramp = True
    truncate_output_file()
    task = test_exec(parameters)
    test_step(check_ramp_output, task.run)
    parameters.command = 'clean'
    truncate_output_file()
    task = test_exec(parameters)
    test_step(None, task.test_clean)


def test_file(parameters, sync=False):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)

    truncate_output_file()
    parameters.command = 'load'
    parameters.count = 100
    parameters.ops = 100
    parameters.replica = 0
    parameters.threads = os.cpu_count()
    parameters.max = os.cpu_count()
    parameters.sync = sync
    parameters.file = package_dir + '/test/test.json'
    parameters.id = "id"
    parameters.bucket = "external"
    parameters.output = "test_output.out"
    truncate_output_file()
    task = test_exec(parameters)
    test_step(check_status_output, task.run)
    parameters.command = 'run'
    parameters.ramp = False
    truncate_output_file()
    task = test_exec(parameters)
    test_step(check_run_output, task.run)
    parameters.ramp = True
    truncate_output_file()
    task = test_exec(parameters)
    test_step(check_ramp_output, task.run)
    parameters.command = 'clean'
    truncate_output_file()
    task = test_exec(parameters)
    test_step(None, task.test_clean)


def cli_run(cmd: str, *args: str):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    run_cmd = [
        cmd,
        *args
    ]

    out_file = open("test_output.out", "a")
    p = subprocess.Popen(run_cmd, stdout=out_file, stderr=out_file, cwd=package_dir, bufsize=1)
    p.communicate()

    if p.returncode != 0:
        print(f"cli test failed {cmd} {' '.join(str(i) for i in args)}")
        raise Exception(f"{cmd} returned non-zero")


def test_cli(hostname, username, password, schema):
    cmd = './cb_perf'
    args = []

    truncate_output_file()
    args.append('load')
    args.append('--host')
    args.append(hostname)
    args.append('-u')
    args.append(username)
    args.append('-p')
    args.append(password)
    args.append('--count')
    args.append('50')
    args.append('--schema')
    args.append(schema)
    args.append('--replica')
    args.append('0')
    test_step(check_status_output, cli_run, cmd, *args)
    truncate_output_file()
    args.clear()
    args.append('clean')
    args.append('--host')
    args.append(hostname)
    args.append('-u')
    args.append(username)
    args.append('-p')
    args.append(password)
    args.append('--schema')
    args.append(schema)
    test_step(check_clean, cli_run, cmd, *args)


def directory_cleanup():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    print("Pruning old test files ... ")
    for file_name in os.listdir(package_dir):
        p1 = re.compile("test_fail_.*\.out")
        p2 = re.compile("test_fail_.*\.log")
        p3 = re.compile("test_output.out")
        if p1.match(file_name) or p2.match(file_name) or p3.match(file_name):
            if file_name == "." or file_name == ".." or file_name == "*" or len(file_name) == 0:
                continue
            print(f"Removing {file_name}")
            os.remove(file_name)
    print("Done.")


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-u', '--user', action='store', help="User Name", default="Administrator")
    parser.add_argument('-p', '--password', action='store', help="User Password", default="password")
    parser.add_argument('-h', '--host', action='store', help="Cluster Node Name", default="localhost")
    parser.add_argument('-b', '--bucket', action='store', help="Test Bucket", default="testrun")
    parser.add_argument('-e', '--external', action='store_true', help='Use external network')
    parser.add_argument('--noapi', action='store_true', help="Disable Capella API functionality")
    parser.add_argument('--help', action='help', default=argparse.SUPPRESS, help='Show help message')
    parser.add_argument('--tls', action='store_true', help="Enable SSL")
    parser.add_argument('--ping', action='store_true', help='Show cluster ping output')
    parser.add_argument('--test', action='store_true', help='Just check status and error if not ready')
    parser.add_argument('--memquota', action='store', help="Bucket Memory Quota", type=int)
    parser.add_argument('--file', action='store', help="File based collection schema JSON")
    parser.add_argument('--id', action='store', help="ID field for file based collection schema", default="record_id")
    parser.add_argument('--debug', action='store', help="Enable Debug Output", type=int, default=3)
    parser.add_argument('--skiprules', action='store_true', help="Do not run rules if defined")
    # parser.add_argument('--sync', action='store_true', help="Use Synchronous Connections")
    parser.add_argument('--schema', action='store', help="Test Schema", default="default")
    parser.add_argument('--noinit', action='store_true', help="Skip init phase")
    parser.add_argument('--safe', action='store_true', help="Do not overwrite data")
    args = parser.parse_args()

    schema_list = [
        'default',
        'profile_demo',
        'employee_demo'
    ]

    username = args.user
    password = args.password
    hostname = args.host
    bucket = args.bucket
    if args.external:
        external = True
    else:
        external = False
    if args.noapi:
        cloud_api = False
    else:
        cloud_api = True

    print(f"cbperf test set v{VERSION}")
    print("Python version:")
    print(sys.version)

    directory_cleanup()

    print("No SSL Tests")
    cb_connect_test(hostname, username, password, bucket, False, external, cloud_api)

    print("SSL Tests")
    cb_connect_test(hostname, username, password, bucket, True, external, cloud_api)

    print("Randomize Tests")
    randomize_test()

    for schema in schema_list:
        print(f"Running tests on schema {schema}")
        print("Main Async Tests")
        test_main(args, sync=False, schema=schema)
        test_file(args, sync=False)
        print("Main Sync Tests")
        test_main(args, sync=True, schema=schema)
        test_file(args, sync=True)

    print("CLI Invoke Tests")
    for schema in schema_list:
        test_cli(hostname, username, password, schema)

    print(f"{tests_run} test(s) run")
    if failed > 0:
        print(f"[!] Not all tests were successful. {failed} test(s) resulted in errors.")
        sys.exit(1)
    else:
        print("All tests were successful")
        sys.exit(0)


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
