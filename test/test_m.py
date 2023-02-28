#!/usr/bin/env python3

import warnings
import sys
import argparse
import os
import logging
from cbcmgr.cb_connect import CBConnect
from cbcmgr.cb_management import CBManager

current = os.path.dirname(os.path.realpath(__file__))
sys.path.append(current)

from conftest import pytest_sessionstart, pytest_sessionfinish

logger = logging.getLogger()

warnings.filterwarnings("ignore")
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


class Params(object):

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--ssl', action='store_true', help="Use SSL")
        parser.add_argument('--host', action='store', help="Hostname or IP address", default="127.0.0.1")
        parser.add_argument('--user', action='store', help="User Name", default="Administrator")
        parser.add_argument('--password', action='store', help="User Password", default="password")
        parser.add_argument('--bucket', action='store', help="Test Bucket", default="testrun")
        parser.add_argument('--start', action='store_true', help="Start Container")
        parser.add_argument('--stop', action='store_true', help="Stop Container")
        self.args = parser.parse_args()

    @property
    def parameters(self):
        return self.args


def container_start():
    pytest_sessionstart(None)


def container_stop():
    pytest_sessionfinish(None, 0)


def manual_1(hostname, bucket, tls, scope, collection):
    replica_count = 0

    print("=> Connect")
    dbm = CBManager(hostname, "Administrator", "password", ssl=False).connect()
    dbm.create_bucket(bucket)
    dbm.create_scope(scope)
    dbm.create_collection(collection)
    dbc = CBConnect(hostname, "Administrator", "password", ssl=False).connect(bucket, scope, collection)
    print("=> Create indexes")
    dbm.cb_create_primary_index(replica=replica_count)
    index_name = dbm.cb_create_index(fields=["data"], replica=replica_count)
    dbm.index_wait()
    dbm.index_wait(index_name)
    result = dbm.is_index()
    assert result is True
    result = dbm.is_index(index_name)
    assert result is True
    dbc.cb_upsert("test::1", document)
    dbc.bucket_wait(bucket, count=1)
    print("=> Data tests")
    result = dbc.cb_get("test::1")
    assert result == document
    result = dbc.collection_count(expect_count=1)
    assert result == 1
    result = dbc.cb_query(field="data", empty_retry=True)
    assert result == query_result
    dbc.cb_upsert("test::2", document)
    dbc.cb_subdoc_multi_upsert(["test::1", "test::2"], "data", ["new", "new"])
    result = dbc.cb_get("test::1")
    assert result == new_document
    result = dbc.collection_count(expect_count=2)
    assert result == 2
    dbc.cb_upsert("test::3", document)
    dbc.cb_subdoc_upsert("test::3", "data", "new")
    result = dbc.cb_get("test::3")
    assert result == new_document
    print("=> Cleanup")
    dbm.cb_drop_primary_index()
    dbm.cb_drop_index(index_name)
    dbm.delete_wait()
    dbm.delete_wait(index_name)
    dbm.drop_bucket(bucket)


p = Params()
options = p.parameters

try:
    debug_level = int(os.environ['CB_PERF_DEBUG_LEVEL'])
except (ValueError, KeyError):
    debug_level = 3

if debug_level == 0:
    logger.setLevel(logging.DEBUG)
elif debug_level == 1:
    logger.setLevel(logging.ERROR)
elif debug_level == 2:
    logger.setLevel(logging.INFO)
else:
    logger.setLevel(logging.CRITICAL)

logging.basicConfig()

if options.start:
    container_start()
    sys.exit(0)

if options.stop:
    container_stop()
    sys.exit(0)

manual_1(options.host, "test", options.ssl, "test", "test")
