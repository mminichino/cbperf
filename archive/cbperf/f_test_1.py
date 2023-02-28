#!/usr/bin/env python3

import os
import sys
import psutil

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
project_dir = os.path.dirname(parent)
sys.path.append(project_dir)

from lib.cbutil import cbsync, cbasync
import pytest
import asyncio
import warnings
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
replica_count = 0
VERSION = "1.0"
warnings.filterwarnings("ignore")


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


def unhandled_exception(loop, context):
    err = context.get("exception", context['message'])
    if isinstance(err, Exception):
        print(f"unhandled exception: type: {err.__class__.__name__} msg: {err} cause: {err.__cause__}")
    else:
        print(f"unhandled error: {err}")


@pytest.mark.parametrize("scope, collection", [("_default", "_default"), ("testscope", "testcollection")])
@pytest.mark.parametrize("tls", [False, True])
def test_sync_1(hostname, username, password, bucket, tls, scope, collection):
    warnings.filterwarnings("ignore")
    global replica_count
    db = cbsync.cb_connect_s(hostname, username, password, ssl=tls).init()

    db.create_bucket(bucket)
    db.bucket_wait(bucket)
    if scope == '_default':
        db.scope()
    else:
        db.create_scope(scope)
        db.scope_wait(scope)
    if collection == '_default':
        db.collection()
    else:
        db.create_collection(collection)
        db.collection_wait(collection)
    result = db.is_bucket(bucket)
    assert result is True
    result = db.is_scope(scope)
    assert result is not None
    result = db.is_collection(collection)
    assert result is not None
    db.cb_create_primary_index(replica=replica_count)
    db.cb_create_index(field="data", replica=replica_count)
    db.index_wait()
    db.index_wait(field="data")
    result = db.is_index()
    assert result is True
    result = db.is_index(field="data")
    assert result is True
    db.cb_upsert("test::1", document)
    db.bucket_wait(bucket, count=1)
    result = db.cb_get("test::1")
    assert result == document
    result = db.collection_count(expect_count=1)
    assert result == 1
    result = db.cb_query(field="data", empty_retry=True)
    assert result == query_result
    db.cb_upsert("test::2", document)
    db.cb_subdoc_multi_upsert(["test::1", "test::2"], "data", ["new", "new"])
    result = db.cb_get("test::1")
    assert result == new_document
    result = db.collection_count(expect_count=2)
    assert result == 2
    db.cb_upsert("test::3", document)
    db.cb_subdoc_upsert("test::3", "data", "new")
    result = db.cb_get("test::3")
    assert result == new_document
    db.cb_drop_primary_index()
    db.cb_drop_index(field="data")
    db.delete_wait()
    db.delete_wait(field="data")
    db.drop_bucket(bucket)
    check_open_files()


@pytest.mark.parametrize("scope, collection", [("_default", "_default"), ("testscope", "testcollection")])
@pytest.mark.parametrize("tls", [False, True])
def test_async_1(hostname, username, password, bucket, tls, scope, collection):
    warnings.filterwarnings("ignore")
    global replica_count
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(unhandled_exception)
    db = loop.run_until_complete(cbasync.cb_connect_a(hostname, username, password, ssl=tls).init())

    loop.run_until_complete(db.create_bucket(bucket))
    loop.run_until_complete(db.bucket_wait(bucket))
    if scope == '_default':
        loop.run_until_complete(db.scope())
    else:
        loop.run_until_complete(db.create_scope(scope))
        loop.run_until_complete(db.scope_wait(scope))
    if collection == '_default':
        loop.run_until_complete(db.collection())
    else:
        loop.run_until_complete(db.create_collection(collection))
        loop.run_until_complete(db.collection_wait(collection))
    result = loop.run_until_complete(db.is_bucket(bucket))
    assert result is True
    result = loop.run_until_complete(db.is_scope(scope))
    assert result is not None
    result = loop.run_until_complete(db.is_collection(collection))
    assert result is not None
    loop.run_until_complete(db.cb_create_primary_index(replica=replica_count))
    loop.run_until_complete(db.cb_create_index(field="data", replica=replica_count))
    loop.run_until_complete(db.index_wait())
    loop.run_until_complete(db.index_wait(field="data"))
    result = loop.run_until_complete(db.is_index())
    assert result is True
    result = loop.run_until_complete(db.is_index(field="data"))
    assert result is True
    loop.run_until_complete(db.cb_upsert("test::1", document))
    loop.run_until_complete(db.bucket_wait(bucket, count=1))
    result = loop.run_until_complete(db.cb_get("test::1"))
    assert result == document
    result = loop.run_until_complete(db.collection_count(expect_count=1))
    assert result == 1
    result = loop.run_until_complete(db.cb_query(field="data", empty_retry=True))
    assert result == query_result
    loop.run_until_complete(db.cb_upsert("test::2", document))
    loop.run_until_complete(db.cb_subdoc_multi_upsert(["test::1", "test::2"], "data", ["new", "new"]))
    result = loop.run_until_complete(db.cb_get("test::1"))
    assert result == new_document
    result = loop.run_until_complete(db.collection_count(expect_count=2))
    assert result == 2
    loop.run_until_complete(db.cb_upsert("test::3", document))
    loop.run_until_complete(db.cb_subdoc_upsert("test::3", "data", "new"))
    result = loop.run_until_complete(db.cb_get("test::3"))
    assert result == new_document
    loop.run_until_complete(db.cb_drop_primary_index())
    loop.run_until_complete(db.cb_drop_index(field="data"))
    loop.run_until_complete(db.delete_wait())
    loop.run_until_complete(db.delete_wait(field="data"))
    loop.run_until_complete(db.drop_bucket(bucket))
    check_open_files()
