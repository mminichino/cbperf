#!/usr/bin/env -S python3 -W ignore

import os
import sys

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

from lib import system
from lib.cbutil import cbconnect
from lib.cbutil import cbindex
import argparse

document = {
    "id": 1,
    "data": "data",
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


def check_list(a, b):
    for item in a:
        if item not in b:
            return False
    return True


def test_step(check, fun, *args, **kwargs):
    global failed, tests_run
    try:
        tests_run += 1
        # args_str = ','.join(map(str, args))
        # kwargs_str = ','.join('{}={}'.format(k, v) for k, v in kwargs.items())
        print(f" {tests_run}) Test {fun.__name__}() ... ", end='')
        result = fun(*args, **kwargs)
        if check:
            if type(check) == list:
                assert check_list(check, result) is True
            else:
                assert check is result
        print("Ok")
    except Exception as err:
        print(f"Step failed: function {fun.__name__}: {err}")
        failed += 1


def cb_sync_test_set(host, username, password, bucket, scope, collection, tls, external_network, cloud_api):
    global replica_count
    db = cbconnect.cb_connect(host, username, password, ssl=tls, external=external_network, cloud=cloud_api)
    db_index = cbindex.cb_index(host, username, password, ssl=tls, external=external_network, cloud=cloud_api)

    test_step(None, db.connect_s)
    test_step(None, db_index.connect)
    test_step(None, db.create_bucket, bucket)
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
    test_step(None, db_index.connect_bucket, bucket)
    test_step(None, db_index.connect_scope, scope)
    test_step(None, db_index.connect_collection, collection)
    test_step(None, db_index.create_index, collection, replica=replica_count)
    test_step(None, db_index.create_index, collection, field="data", index_name="data_index", replica=replica_count)
    test_step(None, db_index.index_wait, collection)
    test_step(None, db_index.index_wait, collection, field="data", index_name="data_index")
    test_step(None, db.cb_upsert_s, "test::1", document, name=collection)
    test_step(query_result, db.cb_query_s, field="data", name=collection)
    test_step(None, db.drop_bucket, bucket)


def cb_connect_test(host, username, password, bucket, tls, external_network, cloud_api):
    print("Default scope and collection tests")
    cb_sync_test_set(host, username, password, bucket, '_default', '_default', tls, external_network, cloud_api)
    print("Named scope and collection tests")
    cb_sync_test_set(host, username, password, bucket, 'testscope', 'testcollection', tls, external_network, cloud_api)


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-u', '--user', action='store', help="User Name", default="Administrator")
    parser.add_argument('-p', '--password', action='store', help="User Password", default="password")
    parser.add_argument('-h', '--host', action='store', help="Cluster Node Name", default="localhost")
    parser.add_argument('-b', '--bucket', action='store', help="Test Bucket", default="testrun")
    parser.add_argument('-e', '--external', action='store_true', help='Use external network')
    parser.add_argument('--noapi', action='store_true', help="Disable Capella API functionality")
    parser.add_argument('--help', action='help', default=argparse.SUPPRESS, help='Show help message')
    args = parser.parse_args()

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

    print("No SSL Tests")
    cb_connect_test(hostname, username, password, bucket, False, external, cloud_api)

    print("SSL Tests")
    cb_connect_test(hostname, username, password, bucket, True, external, cloud_api)

    print(f"{tests_run} test(s) run")
    if failed > 0:
        print(f"Not all tests were successful. {failed} test(s) resulted in errors.")
        sys.exit(1)


if __name__ == '__main__':
    try:
        main()
    except SystemExit as e:
        sys.exit(e.code)
