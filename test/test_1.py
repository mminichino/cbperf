#!/usr/bin/env python3

import os
import subprocess
import re
import warnings

warnings.filterwarnings("ignore")
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)


def cli_run(cmd: str, *args: str, input_file: str = None):
    command_output = ""
    run_cmd = [
        cmd,
        *args
    ]

    p = subprocess.Popen(run_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    if input_file:
        with open(input_file, 'rb') as input_data:
            while True:
                line = input_data.readline()
                if not line:
                    break
                p.stdin.write(line)
            p.stdin.close()

    while True:
        line = p.stdout.readline()
        if not line:
            break
        line_string = line.decode("utf-8")
        command_output += line_string

    p.wait()

    return p.returncode, command_output


def test_cli_1(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['load', '--host', hostname, '--count', '30', '--schema', 'employee_demo', '--replica', '0']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Processing rules")
    assert p.search(output) is not None
    assert result == 0


def test_cli_2(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['clean', '--host', hostname, '--schema', 'employee_demo']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Removing bucket employees")
    assert p.search(output) is not None
    assert result == 0


def test_cli_3(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['load', '--host', hostname, '--count', '1000', '--schema', 'profile_demo', '--replica', '0']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Running link rule rule0")
    assert p.search(output) is not None
    assert result == 0


def test_cli_4(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['clean', '--host', hostname, '--schema', 'profile_demo']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Removing bucket sample_app")
    assert p.search(output) is not None
    assert result == 0


def test_cli_5(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['load', '--host', hostname, '--count', '1000', '--schema', 'default', '--replica', '0']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Processing rules")
    assert p.search(output) is not None
    assert result == 0


def test_cli_6(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['clean', '--host', hostname, '--schema', 'default']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Removing bucket cbperf")
    assert p.search(output) is not None
    assert result == 0


def test_cli_7(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['list', '--host', hostname, '--wait']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Cluster Host List")
    assert p.search(output) is not None
    assert result == 0


def test_cli_8(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['load', '--host', hostname, '--count', '100', '--file', current + '/input_template.json', '--replica', '0']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Processing rules")
    assert p.search(output) is not None
    assert result == 0


def test_cli_9(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['clean', '--host', hostname]

    result, output = cli_run(cmd, *args)
    p = re.compile(r"Removing bucket pillowfight")
    assert p.search(output) is not None
    assert result == 0


def test_cli_10(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['load', '--host', hostname]

    result, output = cli_run(cmd, *args, input_file=current + '/input_stdin.dat')
    p = re.compile(r"Collection had 0 documents - inserted 7 additional record")
    assert p.search(output) is not None
    assert result == 0


def test_cli_11(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['get', '--host', hostname, '-k', 'pillowfight:1']

    result, output = cli_run(cmd, *args)
    p = re.compile(r'"record_id": 1')
    assert p.findall(output) is not None
    assert result == 0


def test_cli_12(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['get', '--host', hostname, '-k', 'pillowfight:%N']

    result, output = cli_run(cmd, *args)
    p = re.compile(r'"record_id": 7')
    assert p.findall(output) is not None
    assert result == 0


def test_cli_13(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['clean', '--host', hostname]

    result, output = cli_run(cmd, *args)
    p = re.compile(r"Removing bucket pillowfight")
    assert p.search(output) is not None
    assert result == 0


def test_cli_14(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['load', '--host', hostname, '--count', '30', '--schema', 'employee_demo', '--replica', '0']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Processing rules")
    assert p.search(output) is not None
    assert result == 0


def test_cli_15(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['export', 'json', '--host', hostname, '-i', '-b', 'employees', '--directory', '/var/tmp']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Retrieved 30 records")
    assert p.search(output) is not None
    assert result == 0


def test_cli_16(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['export', 'csv', '--host', hostname, '-i', '-b', 'employees', '--directory', '/var/tmp']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Retrieved 30 records")
    assert p.search(output) is not None
    assert result == 0


def test_cli_17(hostname):
    global parent
    cmd = parent + '/bin/cb_perf'
    args = ['clean', '--host', hostname, '--schema', 'employee_demo']

    result, output = cli_run(cmd, *args)
    p = re.compile(f"Removing bucket employees")
    assert p.search(output) is not None
    assert result == 0
