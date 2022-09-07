#!/usr/bin/env python3

import os
import subprocess
import re
import json


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


def test_cli_1():
    cmd = './capella_api_cli.py'
    args = []

    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    root_dir = os.path.dirname(package_dir)
    os.chdir(root_dir)

    args.append('-g')
    args.append('-e')
    args.append('/v2/projects')
    result, output = cli_run(cmd, *args)
    try:
        json.loads(output)
    except Exception as err:
        raise Exception(f"test_cli_1 failed: unexpected output: {err}")
    assert result == 0


def test_cli_2():
    cmd = './capella_api_cli.py'
    args = []

    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    root_dir = os.path.dirname(package_dir)
    os.chdir(root_dir)

    args.append('-g')
    args.append('-e')
    args.append('/v3/clusters')
    result, output = cli_run(cmd, *args)
    assert result == 0
    try:
        data = json.loads(output)
        for item in data:
            args.clear()
            args.append('-g')
            args.append('-e')
            args.append(f"/v3/clusters/{item['id']}")
            result, output = cli_run(cmd, *args)
            if result == 404 or len(output) == 0:
                continue
            cluster_data = json.loads(output)
            assert result == 0
    except Exception as err:
        raise Exception(f"test_cli_2 failed: unexpected output: {err}")


def test_cli_3():
    cmd = './capella_api_cli.py'
    args = []

    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    root_dir = os.path.dirname(package_dir)
    os.chdir(root_dir)

    args.append('-g')
    args.append('-e')
    args.append('/v3/clusters')
    result, output = cli_run(cmd, *args)
    assert result == 0
    try:
        data = json.loads(output)
        for item in data:
            args.clear()
            args.append('cluster')
            args.append('get')
            args.append(item['name'])
            result, output = cli_run(cmd, *args)
            p = re.compile(f"Cluster details for {item['name']} were not found")
            if p.search(output) is not None or len(output) == 0:
                continue
            cluster_data = json.loads(output)
            assert result == 0
    except Exception as err:
        raise Exception(f"test_cli_3 failed: unexpected output: {err}")

