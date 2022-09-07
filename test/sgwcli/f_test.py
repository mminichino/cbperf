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
    args.append("testdb")
    result, output = cli_run(cmd, *args)
    p = re.compile(f"^Database testdb does not exist.*$")
    assert p.match(output) is not None
    assert result == 1


def test_cli_2(hostname, bucket):
    cmd = './sgwcli.py'
    args = []

    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    root_dir = os.path.dirname(package_dir)
    os.chdir(root_dir)

    args.append('database')
    args.append('create')
    args.append('-h')
    args.append(hostname)
    args.append('-n')
    args.append("testdb")
    args.append('-b')
    args.append(bucket)
    result, output = cli_run(cmd, *args)
    p = re.compile(f"^Database testdb created for bucket {bucket}.*$")
    assert p.match(output) is not None
    assert result == 0


def test_cli_3(hostname, bucket):
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
    args.append("testdb")
    result, output = cli_run(cmd, *args)
    p = re.compile(f"Bucket:.*{bucket}")
    assert p.search(output) is not None
    p = re.compile(f"Name:.*testdb")
    assert p.search(output) is not None
    p = re.compile(f"Replicas:.*0")
    assert p.search(output) is not None
    assert result == 0


def test_cli_4(hostname, bucket):
    cmd = './sgwcli.py'
    args = []

    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    root_dir = os.path.dirname(package_dir)
    os.chdir(root_dir)

    args.append('database')
    args.append('dump')
    args.append('-h')
    args.append(hostname)
    args.append('-n')
    args.append("testdb")
    result, output = cli_run(cmd, *args)
    p = re.compile(f"Key: .* Id: .*")
    assert p.match(output) is not None
    assert result == 0


def test_cli_5(hostname, bucket):
    cmd = './sgwcli.py'
    args = []

    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    root_dir = os.path.dirname(package_dir)
    os.chdir(root_dir)

    args.append('user')
    args.append('list')
    args.append('-h')
    args.append(hostname)
    args.append('-n')
    args.append("testdb")
    result, output = cli_run(cmd, *args)
    assert len(output) == 0
    assert result == 0


def test_cli_6(hostname, bucket):
    cmd = './sgwcli.py'
    args = []

    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    root_dir = os.path.dirname(package_dir)
    os.chdir(root_dir)

    args.append('user')
    args.append('create')
    args.append('-h')
    args.append(hostname)
    args.append('-n')
    args.append("testdb")
    args.append('--dbuser')
    args.append("demouser")
    args.append('--dbpass')
    args.append("password")
    result, output = cli_run(cmd, *args)
    p = re.compile(f"^User demouser created for database testdb.*$")
    assert p.match(output) is not None
    assert result == 0


def test_cli_7(hostname, bucket):
    cmd = './sgwcli.py'
    args = []

    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    root_dir = os.path.dirname(package_dir)
    os.chdir(root_dir)

    args.append('user')
    args.append('list')
    args.append('-h')
    args.append(hostname)
    args.append('-n')
    args.append("testdb")
    result, output = cli_run(cmd, *args)
    p = re.compile(f"^demouser.*$")
    assert p.match(output) is not None
    assert result == 0


def test_cli_8(hostname, bucket):
    cmd = './sgwcli.py'
    args = []

    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    root_dir = os.path.dirname(package_dir)
    os.chdir(root_dir)

    args.append('user')
    args.append('list')
    args.append('-h')
    args.append(hostname)
    args.append('-n')
    args.append("testdb")
    args.append('--dbuser')
    args.append("demouser")
    result, output = cli_run(cmd, *args)
    p = re.compile(f"Name:.*demouser")
    assert p.search(output) is not None
    p = re.compile(f"Admin channels")
    assert p.search(output) is not None
    p = re.compile(f"All channels")
    assert p.search(output) is not None
    p = re.compile(f"Disabled:.*False")
    assert p.search(output) is not None
    assert result == 0


def test_cli_9(hostname, bucket):
    cmd = './sgwcli.py'
    args = []

    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    root_dir = os.path.dirname(package_dir)
    os.chdir(root_dir)

    args.append('user')
    args.append('delete')
    args.append('-h')
    args.append(hostname)
    args.append('-n')
    args.append("testdb")
    args.append('--dbuser')
    args.append("demouser")
    result, output = cli_run(cmd, *args)
    p = re.compile(f"^User demouser deleted from testdb.*$")
    assert p.match(output) is not None
    assert result == 0


def test_cli_10(hostname, bucket):
    cmd = './sgwcli.py'
    args = []

    current_dir = os.path.dirname(os.path.realpath(__file__))
    package_dir = os.path.dirname(current_dir)
    root_dir = os.path.dirname(package_dir)
    os.chdir(root_dir)

    args.append('database')
    args.append('delete')
    args.append('-h')
    args.append(hostname)
    args.append('-n')
    args.append("testdb")
    result, output = cli_run(cmd, *args)
    p = re.compile(f"^Database testdb deleted.*$")
    assert p.match(output) is not None
    assert result == 0

