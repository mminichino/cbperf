#!/usr/bin/env python3

import os
import sys
import psutil
from .testlibs import CheckCompare

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
project_dir = os.path.dirname(parent)
sys.path.append(project_dir)

from lib.executive import print_host_map, test_exec, schema_admin
import pytest
import warnings
from collections import Counter
import argparse

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


@pytest.mark.parametrize("tls", [False, True])
def test_modules_map_1(hostname, username, password, tls, mocker, capsys):
    warnings.filterwarnings("ignore")
    mocker.patch(
        "sys.argv",
        [
            "cb_perf.py",
            "--host",
            hostname,
            "--user",
            username,
            "--password",
            password,
        ],
    )
    global project_dir
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-u', '--user', action='store', help="User Name", default="Administrator")
    parser.add_argument('-p', '--password', action='store', help="User Password", default="password")
    parser.add_argument('-h', '--host', action='store', help="Cluster Node Name", default="localhost")
    parameters = parser.parse_args()
    os.chdir(project_dir)
    c = CheckCompare()

    parameters.tls = tls
    parameters.file = None
    parameters.schema = "default"
    parameters.id = None
    parameters.bucket = None
    parameters.ping = False
    parameters.noapi = False
    parameters.test = False
    parameters.external = None
    task = print_host_map(parameters)
    task.run()
    captured = capsys.readouterr()
    assert c.check_host_map(captured.out) is True
    check_open_files()
