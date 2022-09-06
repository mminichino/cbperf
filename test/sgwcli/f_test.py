#!/usr/bin/env python3

import os
import subprocess
import re


def cli_run(cmd: str, *args: str):
    command_output = ""
    run_cmd = [
        cmd,
        *args
    ]

    p = subprocess.Popen(run_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    while True:
        line = p.stdout.readline()
        if not line:
            break
        line_string = line.decode("utf-8")
        command_output += line_string

    p.communicate()

    return p.returncode, command_output


def test_cli_1(hostname, bucket):
    cmd = './sgwcli.py'
    args = []

    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    root_dir = os.path.dirname(package_dir)
    os.chdir(root_dir)

    args.append('database')
    args.append('list')
    args.append('-h')
    args.append(hostname)
    args.append('-n')
    args.append(bucket)
    result, output = cli_run(cmd, *args)
    p = re.compile(f"^Database {bucket} does not exist.*$")
    assert p.match(output) is not None
    assert result == 1
