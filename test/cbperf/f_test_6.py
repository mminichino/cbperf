#!/usr/bin/env python3

import os
import sys
import psutil
from .testlibs import CheckCompare

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
project_dir = os.path.dirname(parent)
sys.path.append(project_dir)

from lib.executive import test_exec
import pytest
import time
import sys
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


@pytest.mark.parametrize("syncmode", [False, True])
@pytest.mark.parametrize("schema", ["default", "profile_demo", "employee_demo"])
@pytest.mark.parametrize("tls", [False])
# @pytest.mark.parametrize("syncmode", [False, ])
# @pytest.mark.parametrize("schema", ["default", ])
# @pytest.mark.parametrize("tls", [False, ])
def test_modules_ramp_1(hostname, username, password, tls, schema, syncmode, mocker):
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

    if schema == 'custom_file':
        filetest = True
    else:
        filetest = False

    parameters.tls = tls
    parameters.bucket = "testrun"
    parameters.memquota = None
    parameters.debug = None
    parameters.skiprules = None
    parameters.noinit = None
    parameters.safe = None
    parameters.output = "test_output.out"
    parameters.noapi = False
    parameters.external = None
    parameters.command = 'run'
    parameters.ramp = True
    parameters.count = 30
    parameters.ops = 30
    parameters.replica = 0
    parameters.threads = os.cpu_count()
    parameters.max = os.cpu_count()
    parameters.sync = syncmode
    if filetest:
        parameters.file = project_dir + '/test/test.json'
        parameters.id = "id"
        parameters.bucket = "external"
    else:
        parameters.file = None
        parameters.id = None
        parameters.schema = schema
    c.clear_file("test_output.out")
    stdout = sys.stdout
    task = test_exec(parameters)
    task.run()
    check_output = c.read_file("test_output.out")
    assert c.check_run_output(check_output) is True
    parameters.command = 'clean'
    task = test_exec(parameters)
    task.test_clean()
    time.sleep(0.2)
    sys.stdout = stdout
    check_open_files()
