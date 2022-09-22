#!/usr/bin/env python3

import os
import sys
import psutil

import couchbase.result
from couchbase.management.logic.collections_logic import ScopeSpec, CollectionSpec

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
project_dir = os.path.dirname(parent)
sys.path.append(project_dir)

from lib import system
from lib.cbutil import cbsync, cbasync
from lib.cbutil.randomize import randomize
from lib.executive import print_host_map, test_exec, schema_admin
import pytest
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
from collections import Counter

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
    p = re.compile(r"^ \[[0-9]+\] .*$")
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
    success_found = False
    failure_found = 0

    out_file = open("test_output.out", "r")
    line = out_file.readline()
    while line:
        p = re.compile("Beginning [a]*sync .* test with [0-9]+ instances")
        if p.match(line):
            success_found = False
            result_line = out_file.readline()
            while result_line:
                p = re.compile(".*[0-9]+ of [0-9]+, [0-9]+%")
                if p.match(result_line):
                    result = re.search("[0-9]+ of [0-9]+, [0-9]+%", result_line)
                    percentage = result.group(0).split(',')[1].lstrip().rstrip("%")
                    if int(percentage) != 100:
                        failure_found += 1
                    error_line = out_file.readline()
                    while error_line:
                        p = re.compile("^[0-9]+ errors.*$")
                        if p.match(error_line):
                            result = re.search("^[0-9]+ errors.*$", error_line)
                            err_num = result.group(0).split(' ')[0].strip()
                            if int(err_num) != 0:
                                failure_found += 1
                            success_found = True
                            break
                        error_line = out_file.readline()
                if success_found:
                    break
                result_line = out_file.readline()
        line = out_file.readline()
    if failure_found == 0 and success_found:
        return True
    else:
        print(f"{failure_found} failures ", end='')
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


def check_open_files():
    p = psutil.Process()
    open_files = p.open_files()
    open_count = len(open_files)
    connections = p.connections()
    con_count = len(connections)
    num_fds = p.num_fds()
    children = p.children(recursive=True)
    num_children = len(children)
    print(f"open: {open_count} connections: {con_count} fds: {num_fds} children: {num_children} ", end="")
    status_list = []
    for c in connections:
        status_list.append(c.status)
    status_values = Counter(status_list).keys()
    status_count = Counter(status_list).values()
    for state, count in zip(status_values, status_count):
        print(f"{state}: {count} ", end="")
    print("")


def truncate_output_file():
    file = open("test_output.out", "w")
    file.close()


def unhandled_exception(loop, context):
    err = context.get("exception", context['message'])
    if isinstance(err, Exception):
        print(f"unhandled exception: type: {err.__class__.__name__} msg: {err} cause: {err.__cause__}")
    else:
        print(f"unhandled error: {err}")


# def test_step(check, fun, *args, __name=None, **kwargs):
#     global failed, tests_run
#     result = None
#     fun_name = ""
#     try:
#         tests_run += 1
#
#         if __name:
#             fun_name = __name
#         else:
#             fun_name = fun.__name__
#
#         print(f" {tests_run}) Test {fun_name}() ... ", end='')
#         sys.stdout.flush()
#
#         stdout = sys.stdout
#         sys.stdout = open("test_output.out", "a")
#         if __name:
#             result = fun
#         else:
#             result = fun(*args, **kwargs)
#         sys.stdout = stdout
#
#         if check:
#             if type(check) == list:
#                 assert check_list(check, result) is True
#             elif type(check) == bool and check is True:
#                 assert result is not None
#             elif type(check) == int:
#                 assert int(check) == int(result)
#             elif type(result) == couchbase.result.GetResult:
#                 assert check == result.content_as[dict]
#             elif type(check) == CheckCompare:
#                 assert check.check(result)
#             elif callable(check):
#                 assert check() is True
#             else:
#                 assert check == result
#
#         print("Ok")
#         return result
#     except Exception as err:
#         tb = traceback.format_exc()
#         args_str = ','.join(map(str, args))
#         kwargs_str = ','.join('{}={}'.format(k, v) for k, v in kwargs.items())
#         func_call = f"{fun_name}({','.join([args_str, kwargs_str])})"
#         sys.stdout.flush()
#         with open("test_output.out", "a") as out_file:
#             out_file.write(f"Called = {func_call}\n")
#             out_file.write(f"Type of check value = {type(check)}\n")
#             out_file.write(f"Check = {check}\n")
#             out_file.write(f"Result = {result}\n")
#             out_file.write(tb)
#             out_file.write("\n")
#             out_file.close()
#         check_open_files(dump=True)
#         copyfile("test_output.out", f"test_fail_{fun_name}.out")
#         copyfile("cb_debug.log", f"test_fail_{fun_name}.log")
#         print(f"Step failed: function {fun_name}: {err}")
#         failed += 1
#
#
# async def async_test_step(check, fun, *args, **kwargs):
#     global failed, tests_run
#     result = None
#     fun_name = fun.__name__
#     try:
#         tests_run += 1
#
#         print(f" {tests_run}) Test {fun_name}() ... ", end='')
#
#         stdout = sys.stdout
#         sys.stdout = open("test_output.out", "a")
#         result = await fun(*args, **kwargs)
#         sys.stdout = stdout
#
#         if check:
#             if type(check) == list:
#                 assert check_list(check, result) is True
#             elif type(check) == bool and check is True:
#                 assert result is not None
#             elif type(check) == int:
#                 assert int(check) == int(result)
#             elif type(result) == couchbase.result.GetResult:
#                 assert check == result.content_as[dict]
#             elif type(check) == CheckCompare:
#                 assert check.check(result)
#             elif callable(check):
#                 assert check() is True
#             else:
#                 assert check == result
#
#         print("Ok")
#     except Exception as err:
#         tb = traceback.format_exc()
#         args_str = ','.join(map(str, args))
#         kwargs_str = ','.join('{}={}'.format(k, v) for k, v in kwargs.items())
#         func_call = f"{fun_name}({','.join([args_str, kwargs_str])})"
#         sys.stdout.flush()
#         with open("test_output.out", "a") as out_file:
#             out_file.write(f"Called = {func_call}\n")
#             out_file.write(f"Type of check value = {type(check)}\n")
#             out_file.write(f"Check = {check}\n")
#             out_file.write(f"Result = {result}\n")
#             out_file.write(tb)
#             out_file.write("\n")
#             out_file.close()
#         check_open_files(dump=True)
#         copyfile("test_output.out", f"test_fail_{fun_name}.out")
#         copyfile("cb_debug.log", f"test_fail_{fun_name}.log")
#         print(f"Step failed: function {fun_name}: {err}")
#         failed += 1


# @pytest.mark.parametrize("scope, collection", [("_default", "_default"), ("testscope", "testcollection")])
# @pytest.mark.parametrize("tls", [False, True])
# def test_sync_1(hostname, username, password, bucket, tls, scope, collection):
#     warnings.filterwarnings("ignore")
#     global replica_count
#     db = cbsync.cb_connect_s(hostname, username, password, ssl=tls).init()
# 
#     db.create_bucket(bucket)
#     db.bucket_wait(bucket)
#     if scope == '_default':
#         db.scope()
#     else:
#         db.create_scope(scope)
#         db.scope_wait(scope)
#     if collection == '_default':
#         db.collection()
#     else:
#         db.create_collection(collection)
#         db.collection_wait(collection)
#     result = db.is_bucket(bucket)
#     assert result is True
#     result = db.is_scope(scope)
#     assert result is not None
#     result = db.is_collection(collection)
#     assert result is not None
#     db.cb_create_primary_index(replica=replica_count)
#     db.cb_create_index(field="data", replica=replica_count)
#     db.index_wait()
#     db.index_wait(field="data")
#     result = db.is_index()
#     assert result is True
#     result = db.is_index(field="data")
#     assert result is True
#     db.cb_upsert("test::1", document)
#     db.bucket_wait(bucket, count=1)
#     result = db.cb_get("test::1")
#     assert result == document
#     result = db.collection_count(expect_count=1)
#     assert result == 1
#     result = db.cb_query(field="data", empty_retry=True)
#     assert result == query_result
#     db.cb_upsert("test::2", document)
#     db.cb_subdoc_multi_upsert(["test::1", "test::2"], "data", ["new", "new"])
#     result = db.cb_get("test::1")
#     assert result == new_document
#     result = db.collection_count(expect_count=2)
#     assert result == 2
#     db.cb_upsert("test::3", document)
#     db.cb_subdoc_upsert("test::3", "data", "new")
#     result = db.cb_get("test::3")
#     assert result == new_document
#     db.cb_drop_primary_index()
#     db.cb_drop_index(field="data")
#     db.delete_wait()
#     db.delete_wait(field="data")
#     db.drop_bucket(bucket)
#     check_open_files()
# 
# 
# @pytest.mark.parametrize("scope, collection", [("_default", "_default"), ("testscope", "testcollection")])
# @pytest.mark.parametrize("tls", [False, True])
# def test_async_1(hostname, username, password, bucket, tls, scope, collection):
#     warnings.filterwarnings("ignore")
#     global replica_count
#     loop = asyncio.get_event_loop()
#     loop.set_exception_handler(unhandled_exception)
#     db = loop.run_until_complete(cbasync.cb_connect_a(hostname, username, password, ssl=tls).init())
# 
#     loop.run_until_complete(db.create_bucket(bucket))
#     loop.run_until_complete(db.bucket_wait(bucket))
#     if scope == '_default':
#         loop.run_until_complete(db.scope())
#     else:
#         loop.run_until_complete(db.create_scope(scope))
#         loop.run_until_complete(db.scope_wait(scope))
#     if collection == '_default':
#         loop.run_until_complete(db.collection())
#     else:
#         loop.run_until_complete(db.create_collection(collection))
#         loop.run_until_complete(db.collection_wait(collection))
#     result = loop.run_until_complete(db.is_bucket(bucket))
#     assert result is True
#     result = loop.run_until_complete(db.is_scope(scope))
#     assert result is not None
#     result = loop.run_until_complete(db.is_collection(collection))
#     assert result is not None
#     loop.run_until_complete(db.cb_create_primary_index(replica=replica_count))
#     loop.run_until_complete(db.cb_create_index(field="data", replica=replica_count))
#     loop.run_until_complete(db.index_wait())
#     loop.run_until_complete(db.index_wait(field="data"))
#     result = loop.run_until_complete(db.is_index())
#     assert result is True
#     result = loop.run_until_complete(db.is_index(field="data"))
#     assert result is True
#     loop.run_until_complete(db.cb_upsert("test::1", document))
#     loop.run_until_complete(db.bucket_wait(bucket, count=1))
#     result = loop.run_until_complete(db.cb_get("test::1"))
#     assert result == document
#     result = loop.run_until_complete(db.collection_count(expect_count=1))
#     assert result == 1
#     result = loop.run_until_complete(db.cb_query(field="data", empty_retry=True))
#     assert result == query_result
#     loop.run_until_complete(db.cb_upsert("test::2", document))
#     loop.run_until_complete(db.cb_subdoc_multi_upsert(["test::1", "test::2"], "data", ["new", "new"]))
#     result = loop.run_until_complete(db.cb_get("test::1"))
#     assert result == new_document
#     result = loop.run_until_complete(db.collection_count(expect_count=2))
#     assert result == 2
#     loop.run_until_complete(db.cb_upsert("test::3", document))
#     loop.run_until_complete(db.cb_subdoc_upsert("test::3", "data", "new"))
#     result = loop.run_until_complete(db.cb_get("test::3"))
#     assert result == new_document
#     loop.run_until_complete(db.cb_drop_primary_index())
#     loop.run_until_complete(db.cb_drop_index(field="data"))
#     loop.run_until_complete(db.delete_wait())
#     loop.run_until_complete(db.delete_wait(field="data"))
#     loop.run_until_complete(db.drop_bucket(bucket))
#     check_open_files()


# def cb_connect_test(host, username, password, bucket, tls, external_network, cloud_api):
#     print("Sync - Default scope and collection tests")
#     cb_sync_test_set(host, username, password, bucket, '_default', '_default', tls, external_network, cloud_api)
#     print("Sync - Named scope and collection tests")
#     cb_sync_test_set(host, username, password, bucket, 'testscope', 'testcollection', tls, external_network, cloud_api)
#     print("Async - Default scope and collection tests")
#     cb_async_test_set(host, username, password, bucket, '_default', '_default', tls, external_network, cloud_api)
#     print("Async - Named scope and collection tests")
#     cb_async_test_set(host, username, password, bucket, 'testscope', 'testcollection', tls, external_network, cloud_api)


def test_randomizer_1():
    r = randomize()
    c = CheckCompare()
    c.num_range(0, 9)
    result = r._randomNumber(1)
    assert c.check(result) is True
    c.pattern(r'^[a-z]+$')
    result = r._randomStringLower(8)
    assert c.check(result) is True
    c.pattern(r'^[A-Z]+$')
    result = r._randomStringUpper(8)
    assert c.check(result) is True
    c.pattern(r'^[a-zA-Z0-9]+$')
    result = r._randomHash(8)
    assert c.check(result) is True
    c.num_range(0, 255)
    result = r._randomBits(8)
    assert c.check(result) is True
    c.num_range(1, 12)
    result = r._monthNumber()
    assert c.check(result) is True
    c.num_range(1, 31)
    result = r._monthDay()
    assert c.check(result) is True
    c.num_range(1920, 2022)
    result = r._yearNumber()
    assert c.check(result) is True
    c.pattern(r'^[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9]$')
    result = r.creditCard
    assert c.check(result) is True
    c.pattern(r'^[0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9][0-9][0-9]$')
    result = r.socialSecurityNumber
    assert c.check(result) is True
    c.pattern(r'^[0-9][0-9][0-9]$')
    result = r.threeDigits
    assert c.check(result) is True
    c.pattern(r'^[0-9][0-9][0-9][0-9]$')
    result = r.fourDigits
    assert c.check(result) is True
    c.pattern(r'^[0-9][0-9][0-9][0-9][0-9]$')
    result = r.zipCode
    assert c.check(result) is True
    c.pattern(r'^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$')
    result = r.accountNumner
    assert c.check(result) is True
    c.pattern(r'^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$')
    result = r.numericSequence
    assert c.check(result) is True
    c.pattern(r'^[0-9]+\.[0-9]+$')
    result = r.dollarAmount
    assert c.check(result) is True
    c.boolean()
    result = r.booleanValue
    assert c.check(result) is True
    c.num_range(1920, 2022)
    result = r.yearValue
    assert c.check(result) is True
    c.num_range(1, 12)
    result = r.monthValue
    assert c.check(result) is True
    c.num_range(1, 31)
    result = r.dayValue
    assert c.check(result) is True
    c.time()
    result = r.pastDate
    assert c.check(result) is True
    c.time()
    result = r.dobDate
    assert c.check(result) is True
    c.pattern(r'^[0-9]+/[0-9]+/[0-9]+$')
    past_date = r.pastDate
    result = r.pastDateSlash(past_date)
    assert c.check(result) is True
    c.pattern(r'^[0-9]+-[0-9]+-[0-9]+$')
    past_date = r.pastDate
    result = r.pastDateHyphen(past_date)
    assert c.check(result) is True
    c.pattern(r'^[a-zA-Z]+ [0-9]+ [0-9]+$')
    past_date = r.pastDate
    result = r.pastDateText(past_date)
    assert c.check(result) is True
    c.pattern(r'^[0-9]+/[0-9]+/[0-9]+$')
    past_date = r.dobDate
    result = r.dobSlash(past_date)
    assert c.check(result) is True
    c.pattern(r'^[0-9]+-[0-9]+-[0-9]+$')
    past_date = r.dobDate
    result = r.dobHyphen(past_date)
    assert c.check(result) is True
    c.pattern(r'^[a-zA-Z]+ [0-9]+ [0-9]+$')
    past_date = r.dobDate
    result = r.dobText(past_date)
    assert c.check(result) is True
    c.pattern(r'^[a-zA-Z0-9]+$')
    result = r.hashCode
    assert c.check(result) is True
    c.pattern(r'^[a-zA-Z]+$')
    result = r.firstName
    assert c.check(result) is True
    c.pattern(r'^[a-zA-Z]+$')
    result = r.lastName
    assert c.check(result) is True
    c.pattern(r'^[a-zA-Z]+$')
    result = r.streetType
    assert c.check(result) is True
    c.pattern(r'^[a-zA-Z0-9]+$')
    result = r.streetName
    assert c.check(result) is True
    c.pattern(r'^[0-9]+ [a-zA-Z0-9]+ [a-zA-Z]+$')
    result = r.addressLine
    assert c.check(result) is True
    c.pattern(r'^[a-zA-Z ]+$')
    result = r.cityName
    assert c.check(result) is True
    c.pattern(r'^[A-Z]+$')
    result = r.stateName
    assert c.check(result) is True
    c.pattern(r'^[0-9][0-9][0-9]-555-[0-9][0-9][0-9][0-9]$')
    result = r.phoneNumber
    assert c.check(result) is True
    c.pattern(r'^[0-9]+-[0-9]+-[0-9]+ [0-9]+:[0-9]+:[0-9]+$')
    result = r.dateCode
    assert c.check(result) is True
    c.pattern(r'^[a-z]+$')
    first = r.firstName
    last = r.lastName
    result = r.nickName(first, last)
    assert c.check(result) is True
    c.pattern(r'^[a-z]+\.[a-z]+@[a-z]+\.[a-z]+$')
    first = r.firstName
    last = r.lastName
    result = r.emailAddress(first, last)
    assert c.check(result) is True
    c.pattern(r'^[a-z]+[0-9]+$')
    first = r.firstName
    last = r.lastName
    result = r.userName(first, last)
    assert c.check(result) is True
    r.randImage()
    check_open_files()


# def test_map(args):
#     parameters = args
#
#     debugger = cb_debug(os.path.basename(__file__))
#     debugger.clear()
#     debugger.close()
#
#     print(" -> Testing host map")
#     truncate_output_file()
#     task = print_host_map(parameters)
#     test_step(check_host_map, task.run)
#     check_open_files()
#
#
# def test_load(args, sync=False, schema="default", filetest=False):
#     parameters = args
#     current_dir = os.path.dirname(os.path.realpath(__file__))
#     package_dir = os.path.dirname(current_dir)
#
#     debugger = cb_debug(os.path.basename(__file__))
#     debugger.clear()
#     debugger.close()
#
#     print(" -> Testing load")
#     truncate_output_file()
#     parameters.command = 'load'
#     parameters.count = 100
#     parameters.ops = 100
#     parameters.replica = 0
#     parameters.threads = os.cpu_count()
#     parameters.max = os.cpu_count()
#     parameters.sync = sync
#     if filetest:
#         parameters.file = package_dir + '/test/test.json'
#         parameters.id = "id"
#         parameters.bucket = "external"
#     else:
#         parameters.schema = schema
#     parameters.output = "test_output.out"
#     task = test_exec(parameters)
#     test_step(check_run_output, task.run)
#     parameters.command = 'clean'
#     task = test_exec(parameters)
#     test_step(None, task.test_clean)
#     time.sleep(0.2)
#     check_open_files()
#
#
# def test_run(args, sync=False, schema="default", filetest=False):
#     parameters = args
#     current_dir = os.path.dirname(os.path.realpath(__file__))
#     package_dir = os.path.dirname(current_dir)
#
#     debugger = cb_debug(os.path.basename(__file__))
#     debugger.clear()
#     debugger.close()
#
#     print(" -> Testing run")
#     truncate_output_file()
#     parameters.command = 'run'
#     parameters.count = 100
#     parameters.ops = 100
#     parameters.replica = 0
#     parameters.threads = os.cpu_count()
#     parameters.max = os.cpu_count()
#     parameters.sync = sync
#     if filetest:
#         parameters.file = package_dir + '/test/test.json'
#         parameters.id = "id"
#         parameters.bucket = "external"
#     else:
#         parameters.schema = schema
#     parameters.output = "test_output.out"
#     parameters.ramp = False
#     task = test_exec(parameters)
#     test_step(check_run_output, task.run)
#     parameters.command = 'clean'
#     task = test_exec(parameters)
#     test_step(None, task.test_clean)
#     time.sleep(0.2)
#     check_open_files()
#
#
# def test_ramp(args, sync=False, schema="default", filetest=False):
#     parameters = args
#     current_dir = os.path.dirname(os.path.realpath(__file__))
#     package_dir = os.path.dirname(current_dir)
#
#     debugger = cb_debug(os.path.basename(__file__))
#     debugger.clear()
#     debugger.close()
#
#     print(" -> Testing ramp")
#     truncate_output_file()
#     task = print_host_map(parameters)
#     test_step(check_host_map, task.run)
#     parameters.command = 'run'
#     parameters.count = 100
#     parameters.ops = 100
#     parameters.replica = 0
#     parameters.threads = os.cpu_count()
#     parameters.max = os.cpu_count()
#     parameters.sync = sync
#     if filetest:
#         parameters.file = package_dir + '/test/test.json'
#         parameters.id = "id"
#         parameters.bucket = "external"
#     else:
#         parameters.schema = schema
#     parameters.output = "test_output.out"
#     parameters.ramp = True
#     task = test_exec(parameters)
#     test_step(check_run_output, task.run)
#     parameters.command = 'clean'
#     task = test_exec(parameters)
#     test_step(None, task.test_clean)
#     time.sleep(0.2)
#     check_open_files()
#
#
# def test_file(args, sync=False):
#     parameters = args
#     current_dir = os.path.dirname(os.path.realpath(__file__))
#     package_dir = os.path.dirname(current_dir)
#
#     debugger = cb_debug(os.path.basename(__file__))
#     debugger.clear()
#     debugger.close()
#
#     truncate_output_file()
#     parameters.command = 'load'
#     parameters.count = 100
#     parameters.ops = 100
#     parameters.replica = 0
#     parameters.threads = os.cpu_count()
#     parameters.max = os.cpu_count()
#     parameters.sync = sync
#     parameters.file = package_dir + '/test/test.json'
#     parameters.id = "id"
#     parameters.bucket = "external"
#     parameters.output = "test_output.out"
#     truncate_output_file()
#     task = test_exec(parameters)
#     test_step(check_status_output, task.run)
#     parameters.command = 'run'
#     parameters.ramp = False
#     truncate_output_file()
#     task = test_exec(parameters)
#     test_step(check_run_output, task.run)
#     parameters.ramp = True
#     truncate_output_file()
#     task = test_exec(parameters)
#     test_step(check_ramp_output, task.run)
#     parameters.command = 'clean'
#     truncate_output_file()
#     task = test_exec(parameters)
#     test_step(None, task.test_clean)
#     time.sleep(0.2)
#     check_open_files()


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


# def test_cli(hostname, username, password, schema):
#     cmd = './cb_perf'
#     args = []
#
#     truncate_output_file()
#     args.append('load')
#     args.append('--host')
#     args.append(hostname)
#     args.append('-u')
#     args.append(username)
#     args.append('-p')
#     args.append(password)
#     args.append('--count')
#     args.append('50')
#     args.append('--schema')
#     args.append(schema)
#     args.append('--replica')
#     args.append('0')
#     test_step(check_status_output, cli_run, cmd, *args)
#     truncate_output_file()
#     args.clear()
#     args.append('clean')
#     args.append('--host')
#     args.append(hostname)
#     args.append('-u')
#     args.append(username)
#     args.append('-p')
#     args.append(password)
#     args.append('--schema')
#     args.append(schema)
#     test_step(check_clean, cli_run, cmd, *args)
#     check_open_files()


def directory_cleanup():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    print("Pruning old test files ... ")
    for file_name in os.listdir(package_dir):
        p1 = re.compile(r"test_fail_.*\.out")
        p2 = re.compile(r"test_fail_.*\.log")
        p3 = re.compile(r"test_output.out")
        if p1.match(file_name) or p2.match(file_name) or p3.match(file_name):
            if file_name == "." or file_name == ".." or file_name == "*" or len(file_name) == 0:
                continue
            print(f"Removing {file_name}")
            os.remove(file_name)
    print("Done.")


# def main():
#     p = psutil.Process()
#     pid = p.pid
#     parser = argparse.ArgumentParser(add_help=False)
#     parser.add_argument('-u', '--user', action='store', help="User Name", default="Administrator")
#     parser.add_argument('-p', '--password', action='store', help="User Password", default="password")
#     parser.add_argument('-h', '--host', action='store', help="Cluster Node Name", default="localhost")
#     parser.add_argument('-b', '--bucket', action='store', help="Test Bucket", default="testrun")
#     parser.add_argument('-e', '--external', action='store_true', help='Use external network')
#     parser.add_argument('--noapi', action='store_true', help="Disable Capella API functionality")
#     parser.add_argument('--help', action='help', default=argparse.SUPPRESS, help='Show help message')
#     parser.add_argument('--tls', action='store_true', help="Enable SSL")
#     parser.add_argument('--ping', action='store_true', help='Show cluster ping output')
#     parser.add_argument('--test', action='store_true', help='Just check status and error if not ready')
#     parser.add_argument('--memquota', action='store', help="Bucket Memory Quota", type=int)
#     parser.add_argument('--file', action='store', help="File based collection schema JSON")
#     parser.add_argument('--id', action='store', help="ID field for file based collection schema", default="record_id")
#     parser.add_argument('--debug', action='store', help="Enable Debug Output", type=int, default=3)
#     parser.add_argument('--skiprules', action='store_true', help="Do not run rules if defined")
#     # parser.add_argument('--sync', action='store_true', help="Use Synchronous Connections")
#     parser.add_argument('--schema', action='store', help="Test Schema", default="default")
#     parser.add_argument('--noinit', action='store_true', help="Skip init phase")
#     parser.add_argument('--safe', action='store_true', help="Do not overwrite data")
#     args = parser.parse_args()
#
#     schema_list = [
#         'default',
#         'profile_demo',
#         'employee_demo'
#     ]
#
#     username = args.user
#     password = args.password
#     hostname = args.host
#     bucket = args.bucket
#     if args.external:
#         external = True
#     else:
#         external = False
#     if args.noapi:
#         cloud_api = False
#     else:
#         cloud_api = True
#
#     print(f"cbperf test set v{VERSION}")
#     print(f"PID: {pid}")
#     print("Python version: ", end='')
#     print(sys.version)
#
#     directory_cleanup()
#     debugger = cb_debug(os.path.basename(__file__))
#     debugger.clear()
#     debugger.close()
#
#     print("No SSL Tests")
#     cb_connect_test(hostname, username, password, bucket, False, external, cloud_api)
#
#     print("SSL Tests")
#     cb_connect_test(hostname, username, password, bucket, True, external, cloud_api)
#
#     print("Randomize Tests")
#     randomize_test()
#
#     for schema in schema_list:
#         print(f"Running tests on schema {schema}")
#         print(f"Main Async Tests - {schema}")
#         test_map(args)
#         test_load(args, sync=False, schema=schema)
#         test_run(args, sync=False, schema=schema)
#         test_ramp(args, sync=False, schema=schema)
#         print(f"Main Sync Tests - {schema}")
#         test_map(args)
#         test_load(args, sync=True, schema=schema)
#         test_run(args, sync=True, schema=schema)
#         test_ramp(args, sync=True, schema=schema)
#
#     print("External file async test")
#     test_map(args)
#     test_load(args, sync=False, filetest=True)
#     test_run(args, sync=False, filetest=True)
#     test_ramp(args, sync=False, filetest=True)
#     print("External file sync test")
#     test_map(args)
#     test_load(args, sync=True, filetest=True)
#     test_run(args, sync=True, filetest=True)
#     test_ramp(args, sync=True, filetest=True)
#
#     print("CLI Invoke Tests")
#     for schema in schema_list:
#         test_cli(hostname, username, password, schema)
#
#     print(f"{tests_run} test(s) run")
#     if failed > 0:
#         print(f"[!] Not all tests were successful. {failed} test(s) resulted in errors.")
#         sys.exit(1)
#     else:
#         print("All tests were successful")
#         sys.exit(0)
#
#
# if __name__ == '__main__':
#     try:
#         main()
#     except SystemExit as e:
#         sys.exit(e.code)
