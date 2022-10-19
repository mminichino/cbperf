#!/usr/bin/env python3

import os
import sys
import psutil
from .testlibs import CheckCompare

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
project_dir = os.path.dirname(parent)
sys.path.append(project_dir)

from lib.cbutil.randomize import randomize
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
