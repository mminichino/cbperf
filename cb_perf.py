#!/usr/bin/env -S python3 -W ignore

'''
Couchbase Performance Utility
'''

import os
import sys
import argparse
import json
from couchbase.diagnostics import PingState
from jinja2 import Template
import time
import asyncio
import acouchbase.cluster
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from datetime import datetime, timedelta
import random
import socket
import configparser
import couchbase
import logging
from couchbase_core._libcouchbase import LOCKMODE_EXC, LOCKMODE_NONE, LOCKMODE_WAIT
from couchbase.cluster import Cluster, ClusterOptions, QueryOptions, ClusterTimeoutOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import QueryOptions
from couchbase.cluster import QueryIndexManager
from couchbase.management.buckets import CreateBucketSettings, BucketType
from couchbase.exceptions import DocumentNotFoundException
from couchbase.exceptions import CouchbaseException
from couchbase.exceptions import ParsingFailedException
import multiprocessing
from queue import Empty, Full
import psutil
import dns.resolver
import ipaddress
threadLock = multiprocessing.Lock()

LOAD_DATA = 0x0000
KV_TEST = 0x0001
QUERY_TEST = 0x0002
REMOVE_DATA = 0x0003
PAUSE_TEST = 0x0009
INSTANCE_MAX = 0x200
RUN_STOP = 0xFFFF
VERSION = '1.1-alpha'

DEFAULT_JSON = {
    'record_id': 'record_id',
    'first_name': '{{ rand_first }}',
    'last_name': '{{ rand_last }}',
    'address': '{{ rand_address }}',
    'city': '{{ rand_city }}',
    'state': '{{ rand_state }}',
    'zip_code': '{{ rand_zip_code }}',
    'phone': '{{ rand_phone }}',
    'ssn': '{{ rand_ssn }}',
    'dob': "{{ rand_dob_1 }}",
    'account_number': '{{ rand_account }}',
    'card_number': '{{ rand_credit_card }}',
    'transactions': [
        {
            'id': '{{ rand_id }}',
            'date': '{{ rand_date_1 }}',
            'amount': '{{ rand_dollar }}',
        },
    ]
}

class randomize(object):

    def __init__(self):
        self.nowTime = datetime.now()
        self.datetimestr = self.nowTime.strftime("%Y-%m-%d %H:%M:%S")

    def _randomNumber(self, n):
        min_lc = ord(b'0')
        len_lc = 10
        ba = bytearray(random.getrandbits(8) for i in range(n))
        for i, b in enumerate(ba):
            ba[i] = min_lc + b % len_lc
        return ba.decode('utf-8')

    def _randomStringLower(self, n):
        min_lc = ord(b'a')
        len_lc = 26
        ba = bytearray(random.getrandbits(8) for i in range(n))
        for i, b in enumerate(ba):
            ba[i] = min_lc + b % len_lc
        return ba.decode('utf-8')

    def _randomStringUpper(self, n):
        min_lc = ord(b'A')
        len_lc = 26
        ba = bytearray(random.getrandbits(8) for i in range(n))
        for i, b in enumerate(ba):
            ba[i] = min_lc + b % len_lc
        return ba.decode('utf-8')

    def _randomHash(self, n):
        ba = bytearray(random.getrandbits(8) for i in range(n))
        for i, b in enumerate(ba):
            min_lc = ord(b'0') if b < 85 else ord(b'A') if b < 170 else ord(b'a')
            len_lc = 10 if b < 85 else 26
            ba[i] = min_lc + b % len_lc
        return ba.decode('utf-8')

    def _randomBits(self, n):
        yield random.getrandbits(n)

    def _monthNumber(self):
        value = (random.getrandbits(3) + 1) + (random.getrandbits(2) + 1)
        return value

    def _monthDay(self, n=31):
        value = next((i for i in self._randomBits(5) if i >= 1 and i <= n), 1)
        return value

    def _yearNumber(self):
        value = int(self._randomNumber(2)) + 1920
        return value

    @property
    def creditCard(self):
        return '-'.join(self._randomNumber(4) for _ in range(4))

    @property
    def socialSecurityNumber(self):
        return '-'.join([self._randomNumber(3), self._randomNumber(2), self._randomNumber(4)])

    @property
    def threeDigits(self):
        return self._randomNumber(3)

    @property
    def fourDigits(self):
        return self._randomNumber(4)

    @property
    def zipCode(self):
        return self._randomNumber(5)

    @property
    def accountNumner(self):
        return self._randomNumber(10)

    @property
    def numericSequence(self):
        return self._randomNumber(16)

    @property
    def dollarAmount(self):
        value = random.getrandbits(8) % 5 + 1
        return self._randomNumber(value) + '.' + self._randomNumber(2)

    @property
    def yearValue(self):
        return str(self._yearNumber())

    @property
    def monthValue(self):
        value = self._monthNumber()
        return f'{value:02}'

    @property
    def dayValue(self):
        value = self._monthDay()
        return f'{value:02}'

    @property
    def pastDateSlash(self):
        past_date = datetime.today() - timedelta(days=random.getrandbits(12))
        return past_date.strftime("%m/%d/%Y")

    @property
    def pastDateHyphen(self):
        past_date = datetime.today() - timedelta(days=random.getrandbits(12))
        return past_date.strftime("%m-%d-%Y")

    @property
    def pastDateText(self):
        past_date = datetime.today() - timedelta(days=random.getrandbits(12))
        return past_date.strftime("%b %d %Y")

    @property
    def dobSlash(self):
        past_date = datetime.today() - timedelta(days=random.getrandbits(14), weeks=1040)
        return past_date.strftime("%m/%d/%Y")

    @property
    def dobHyphen(self):
        past_date = datetime.today() - timedelta(days=random.getrandbits(14), weeks=1040)
        return past_date.strftime("%m-%d-%Y")

    @property
    def dobText(self):
        past_date = datetime.today() - timedelta(days=random.getrandbits(14), weeks=1040)
        return past_date.strftime("%b %d %Y")

    @property
    def hashCode(self):
        return self._randomHash(16)

    @property
    def firstName(self):
        data = [
            'James',
            'Robert',
            'John',
            'Michael',
            'William',
            'David',
            'Richard',
            'Joseph',
            'Thomas',
            'Charles',
            'Mary',
            'Patricia',
            'Jennifer',
            'Linda',
            'Elizabeth',
            'Barbara',
            'Susan',
            'Jessica',
            'Sarah',
            'Karen',
        ]
        rand_gen = fastRandom(len(data), 0)
        return data[rand_gen.value]

    @property
    def lastName(self):
        data = [
            'Smith',
            'Johnson',
            'Williams',
            'Brown',
            'Jones',
            'Garcia',
            'Miller',
            'Davis',
            'Rodriguez',
            'Martinez',
            'Hernandez',
            'Lopez',
            'Gonzalez',
            'Wilson',
            'Anderson',
            'Thomas',
            'Taylor',
            'Moore',
            'Jackson',
            'Martin',
        ]
        rand_gen = fastRandom(len(data), 0)
        return data[rand_gen.value]

    @property
    def streetType(self):
        data = [
            'Street',
            'Road',
            'Lane',
            'Court',
            'Avenue',
            'Parkway',
            'Trail',
            'Way',
            'Drive',
        ]
        rand_gen = fastRandom(len(data), 0)
        return data[rand_gen.value]

    @property
    def streetName(self):
        data = [
            'Main',
            'Church',
            'Liberty',
            'Park',
            'Prospect',
            'Pine',
            'River',
            'Elm',
            'High',
            'Union',
            'Willow',
            'Dogwood',
            'New',
            'North',
            'South',
            'East',
            'West',
            '1st',
            '2nd',
            '3rd',
        ]
        rand_gen = fastRandom(len(data), 0)
        return data[rand_gen.value]

    @property
    def addressLine(self):
        return ' '.join([self._randomNumber(4), self.streetName, self.streetType])

    @property
    def cityName(self):
        data = [
            'Mannorburg',
            'New Highworth',
            'Salttown',
            'Farmingchester',
            'East Sagepool',
            'Strongdol',
            'Weirton',
            'Hapwich',
            'Lunfield Park',
            'Cruxbury',
            'Oakport',
            'Chatham',
            'Beachborough',
            'Farmingbury Falls',
            'Trinsdale',
            'Wingview',
        ]
        rand_gen = fastRandom(len(data), 0)
        return data[rand_gen.value]

    @property
    def stateName(self):
        data = [
            'AL',
            'AK',
            'AZ',
            'AR',
            'CA',
            'CZ',
            'CO',
            'CT',
            'DE',
            'DC',
            'FL',
            'GA',
            'GU',
            'HI',
            'ID',
            'IL',
            'IN',
            'IA',
            'KS',
            'KY',
            'LA',
            'ME',
            'MD',
            'MA',
            'MI',
            'MN',
            'MS',
            'MO',
            'MT',
            'NE',
            'NV',
            'NH',
            'NJ',
            'NM',
            'NY',
            'NC',
            'ND',
            'OH',
            'OK',
            'OR',
            'PA',
            'PR',
            'RI',
            'SC',
            'SD',
            'TN',
            'TX',
            'UT',
            'VT',
            'VI',
            'VA',
            'WA',
            'WV',
            'WI',
            'WY',
        ]
        rand_gen = fastRandom(len(data), 0)
        return data[rand_gen.value]

    @property
    def phoneNumber(self):
        return '-'.join([self._randomNumber(3), self._randomNumber(3), self._randomNumber(4)])

    @property
    def dateCode(self):
        return self.datetimestr

    def testAll(self):
        print("Credit Card: " + self.creditCard)
        print("SSN        : " + self.socialSecurityNumber)
        print("Four Digits: " + self.fourDigits)
        print("ZIP Code   : " + self.zipCode)
        print("Account    : " + self.accountNumner)
        print("Dollar     : " + self.dollarAmount)
        print("Sequence   : " + self.numericSequence)
        print("Hash       : " + self.hashCode)
        print("Address    : " + self.addressLine)
        print("City       : " + self.cityName)
        print("State      : " + self.stateName)
        print("First      : " + self.firstName)
        print("Last       : " + self.lastName)
        print("Phone      : " + self.phoneNumber)
        print("Date       : " + self.dateCode)
        print("Year       : " + self.yearValue)
        print("Month      : " + self.monthValue)
        print("Day        : " + self.dayValue)
        print("Past Date 1: " + self.pastDateSlash)
        print("Past Date 2: " + self.pastDateHyphen)
        print("Past Date 3: " + self.pastDateText)
        print("DOB Date 1 : " + self.dobSlash)
        print("DOB Date 2 : " + self.dobHyphen)
        print("DOB Date 3 : " + self.dobText)

    def prepareTemplate(self, json_block):
        self.template = json.dumps(json_block)
        self.compiled = Template(self.template)

    def processTemplate(self):
        formattedBlock = self.compiled.render(date_time=self.dateCode,
                                              rand_credit_card=self.creditCard,
                                              rand_ssn=self.socialSecurityNumber,
                                              rand_four=self.fourDigits,
                                              rand_account=self.accountNumner,
                                              rand_id=self.numericSequence,
                                              rand_zip_code=self.zipCode,
                                              rand_dollar=self.dollarAmount,
                                              rand_hash=self.hashCode,
                                              rand_address=self.addressLine,
                                              rand_city=self.cityName,
                                              rand_state=self.stateName,
                                              rand_first=self.firstName,
                                              rand_last=self.lastName,
                                              rand_phone=self.phoneNumber,
                                              rand_year=self.yearValue,
                                              rand_month=self.monthValue,
                                              rand_day=self.dayValue,
                                              rand_date_1=self.pastDateSlash,
                                              rand_date_2=self.pastDateHyphen,
                                              rand_date_3=self.pastDateText,
                                              rand_dob_1=self.dobSlash,
                                              rand_dob_2=self.dobHyphen,
                                              rand_dob_3=self.dobText,
                                              )
        finished = formattedBlock.encode('ascii')
        jsonBlock = json.loads(finished)
        return jsonBlock

class debugOutput(object):

    def __init__(self):
        self.threads = {}
        try:
            self.statDebug = open("stats.debug", 'w')
            self.telemetryDebug = open("telemetry.debug", 'w')
        except Exception as e:
            print("Debug: can not open debug files: %s" % str(e))
            sys.exit(1)

    def threadSet(self, thread):
        fileDesc = "query" + str(thread)
        fileName = fileDesc + ".debug"
        try:
            self.threads[fileDesc] = open(fileName, 'w')
        except Exception as e:
            print("Debug: can not open debug file stats.debug: %s" % str(e))
            sys.exit(1)

    def writeStatDebug(self, text):
        try:
            self.statDebug.write(str(text) + "\n")
        except Exception as e:
            print("writeStatDebug: can not write to file: %s" % str(e))
            sys.exit(1)

    def writeTelemetryDebug(self, blob):
        try:
            # json.dump(blob, self.telemetryDebug)
            self.telemetryDebug.write(str(blob) + "\n")
        except Exception as e:
            print("writeTelemetryDebug: can not write to file: %s" % str(e))
            sys.exit(1)

    def writeQueryDebug(self, blob, thread):
        fileDesc = "query" + str(thread)
        try:
            # json.dump(blob, self.threads[fileDesc])
            self.threads[fileDesc].write(str(blob) + "\n")
        except Exception as e:
            print("writeQueryDebug: can not write to file: %s" % str(e))
            sys.exit(1)

class mpAtomicCounter(object):

    def __init__(self, i=0):
        self.count = multiprocessing.Value('i', i)

    def increment(self, i=1):
        with self.count.get_lock():
            self.count.value += i

    @property
    def value(self):
        return self.count.value

class mpAtomicIncrement(object):

    def __init__(self, i=1):
        self.count = multiprocessing.Value('i', i)

    def reset(self, i=1):
        with self.count.get_lock():
            self.count.value = i

    @property
    def next(self):
        with self.count.get_lock():
            current = self.count.value
            self.count.value += 1
        return current

class rwMixer(object):

    def __init__(self, x=100):
        percentage = x / 100
        if percentage > 0:
            self.factor = 1 / percentage
        else:
            self.factor = 0

    def write(self, n=1):
        if self.factor > 0:
            remainder = n % self.factor
        else:
            remainder = 1
        if remainder == 0:
            return True
        else:
            return False

    def read(self, n=1):
        if self.factor > 0:
            remainder = n % self.factor
        else:
            remainder = 1
        if remainder != 0:
            return True
        else:
            return False

class fastRandom(object):

    def __init__(self, x=256, start=1):
        self.max_value = x
        self.bits = self.max_value.bit_length()
        self.start_value = start

    @property
    def value(self):
        rand_number = random.getrandbits(self.bits) % self.max_value
        if rand_number < self.start_value:
            rand_number = self.start_value
        if rand_number > self.max_value:
            rand_number = self.max_value
        return rand_number

class NotAuthorized(Exception):
    def __init__(self, message):
        super().__init__(message)

class RequestNotFound(Exception):
    def __init__(self, message):
        super().__init__(message)

class cbutil(object):

    def __init__(self, hostname='localhost', username='Administrator', password='password', ssl=False, aio=False,
                 internal=False):
        import logging
        self.debug = False
        self.tls = False
        self.logger = None
        self.aio = aio
        self.hostname = hostname
        self.username = username
        self.password = password
        self.bucket_memory = None
        self.host_list = []
        self.ext_host_list = []
        self.srv_host_list = []
        self.rally_point_hostname = hostname
        self.connect_name = hostname
        self.mem_quota = None
        self.retries = 5
        self.auth = PasswordAuthenticator(self.username, self.password)
        self.timeouts = ClusterTimeoutOptions(query_timeout=timedelta(seconds=4800), kv_timeout=timedelta(seconds=4800))
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cluster_s = None
        self.cluster_a = None
        self.bucket_s = None
        self.bucket_a = None
        self.collection_s = None
        self.collection_a = None
        self.bm = None
        self.qim = None
        if internal:
            net_string = 'default'
        else:
            net_string = 'external'
        if ssl:
            self.tls = True
            self.url = "https://"
            self.aport = ":18091"
            self.nport = ":19102"
            self.cbcon = "couchbases://"
            self.opts = "?ssl=no_verify&config_total_timeout=15&config_node_timeout=10&network=" + net_string
        else:
            self.url = "http://"
            self.aport = ":8091"
            self.nport = ":9102"
            self.cbcon = "couchbase://"
            self.opts = "?config_total_timeout=15&config_node_timeout=10&network=" + net_string

        if not self.is_reachable():
            self.logger.error("cbutil: %s is unreachable" % hostname)
            raise Exception("Can not connect to %s." % hostname)

        try:
            self.get_hostlist()
        except Exception as e:
            self.logger.error("cbutil: %s" % str(e))
            raise

    def set_debug(self, level=2):
        if level == 0:
            self.logger.setLevel(logging.DEBUG)
        elif level == 1:
            self.logger.setLevel(logging.INFO)
        elif level == 2:
            self.logger.setLevel(logging.ERROR)
        else:
            self.logger.setLevel(logging.CRITICAL)

    def check_status_code(self, code):
        if code == 200 or code == 201 or code == 202 or code == 204:
            return True
        elif code == 401:
            self.logger.error("check_status_code: code %d" % code)
            raise NotAuthorized("Unauthorized: Invalid credentials")
        elif code == 403:
            self.logger.error("check_status_code: code %d" % code)
            raise NotAuthorized("Forbidden: Insufficient privileges")
        elif code == 404:
            self.logger.error("check_status_code: code %d" % code)
            raise RequestNotFound("Not Found")
        else:
            self.logger.error("check_status_code: code %d" % code)
            raise Exception("Request Failed: Response Code %d" % code)

    @property
    def cb_string(self):
        return self.cbcon + self.rally_point_hostname + self.opts

    @property
    def admin_url(self):
        return self.url + self.connect_name + self.aport

    def node_url(self, nodename):
        return self.url + nodename + self.nport

    @property
    def version(self):
        return self.sw_version

    @property
    def memquota(self):
        return self.mem_quota
    
    def is_bucket(self, bucket):
        cluster = self.connect_s()
        bm = self.get_bm(cluster)
        try:
            self.logger.debug("is_bucket: checking if %s bucket exists" % bucket)
            result = bm.get_bucket(bucket)
            self.logger.debug("is_bucket: bucket %s exists" % bucket)
            return True
        except Exception as e:
            self.logger.debug("is_bucket: bucket %s does not exist" % bucket)
            return False

    def create_bucket(self, bucket, mem_quota=512):
        cluster = self.connect_s()
        bm = self.get_bm(cluster)
        retries = 0
        if not self.is_bucket(bucket):
            self.logger.info("Creating bucket %s." % bucket)
            try:
                bm.create_bucket(CreateBucketSettings(name=bucket, bucket_type=BucketType.COUCHBASE,
                                                      ram_quota_mb=mem_quota))
                while True:
                    try:
                        time.sleep(0.1)
                        self.logger.debug("create_bucket: trying to get bucket settings")
                        result = bm.get_bucket(bucket)
                        cluster.disconnect()
                        return True
                    except Exception as e:
                        self.logger.debug("create_bucket: can not get settings: %s" % str(e))
                        if retries == self.retries:
                            self.logger.error("create_bucket: timeout: %s." % str(e))
                            raise Exception("Timeout waiting for bucket: %s" % str(e))
                        else:
                            retries += 1
                            time.sleep(0.01 * retries)
                            continue
            except Exception as e:
                self.logger.error("create_bucket: error: %s" % str(e))
                raise Exception("Can not create bucket: %s" % str(e))
        else:
            self.logger.info("Bucket %s exists." % bucket)
            cluster.disconnect()
            return True

    def bucket_count(self, bucket):
        session = requests.Session()
        retries = Retry(total=60,
                        backoff_factor=0.1,
                        status_forcelist=[500, 501, 503])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))

        response = requests.get(self.admin_url + '/pools/default/buckets/' + bucket,
                                auth=(self.username, self.password), verify=False, timeout=15)
        response_json = json.loads(response.text)

        try:
            self.check_status_code(response.status_code)
        except Exception as e:
            self.logger.error("bucket_count: %s" % str(e))
            raise

        try:
            document_count = response_json['basicStats']['itemCount']
        except KeyError:
            self.logger.error("bucket_count: can not parse response")
            raise Exception("Can not get bucket stats. Invalid Response")

        return document_count

    def is_index(self, bucket, index):
        cluster = self.connect_s()
        qim = self.get_qim(cluster)
        try:
            indexList = qim.get_all_indexes(bucket)
            for i in range(len(indexList)):
                if indexList[i].name == index:
                    return True
        except Exception as e:
            self.logger.error("is_index: error: %s" % str(e))
            raise Exception("Could not get index status: %s" % str(e))
        return False

    def create_index(self, bucket, field, index, replica=1):
        retries = 0
        cluster = self.connect_s()
        self.logger.info("Creating index %s on field %s." % (index, field))
        queryText = 'CREATE INDEX ' + index + ' ON ' + bucket + '(' + field + ') WITH {"num_replica": ' + str(replica) + '};'
        while True:
            if self.is_bucket(bucket) and not self.is_index(bucket, index):
                try:
                    result = cluster.query(queryText, QueryOptions(metrics=True))
                    run_time = result.metadata().metrics().execution_time().microseconds
                    run_time = run_time / 1000000
                    self.logger.info(
                        "Index creation for \"%s\" on \"%s\" complete: run time: %f secs" % (index, field, run_time))
                    cluster.disconnect()
                    time.sleep(0.1)
                    return True
                except Exception as e:
                    if retries == self.retries:
                        self.logger.error("create_index: error: %s" % str(e))
                        raise Exception("Could not create index: %s" % str(e))
                    else:
                        retries += 1
                        time.sleep(0.01 * retries)
                        continue
            else:
                return True

    def index_stats(self, bucket):
        index_data = {}
        session = requests.Session()
        retries = Retry(total=60,
                        backoff_factor=0.1,
                        status_forcelist=[500, 501, 503])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))

        for hostname in self.cluster_hosts():
            response = session.get(self.node_url(hostname) + '/api/v1/stats/' + bucket,
                                    auth=(self.username, self.password), verify=False, timeout=15)

            try:
                self.check_status_code(response.status_code)
            except RequestNotFound:
                continue
            except Exception as e:
                self.logger.error("index_stats: %s" % str(e))
                raise

            response_json = json.loads(response.text)

            for key in response_json:
                keyspace, index_name = key.split(':')
                index_name = index_name.split(' ')[0]
                if index_name not in index_data:
                    index_data[index_name] = {}
                for attribute in response_json[key]:
                    if attribute not in index_data[index_name]:
                        index_data[index_name][attribute] = response_json[key][attribute]
                    else:
                        index_data[index_name][attribute] += response_json[key][attribute]

        return index_data

    def index_wait(self, bucket, index, count=1, timeout=120):
        index_wait = 0
        index_data = self.index_stats(bucket)
        if index not in index_data:
            self.logger.error("index_wait: index %s does not exist." % index)
            raise Exception("Index %s does not exist." % index)
        self.logger.info("Waiting for %d document(s) to be indexed." % count)
        while index_data[index]['num_docs_indexed'] < count:
            index_wait += 1
            if index_wait == timeout:
                self.logger.error("index_wait: timeout waiting for documents to be indexed.")
                raise Exception("Timeout waiting for documents to index")
            time.sleep(0.1 * index_wait)
            index_data = self.index_stats(bucket)
        self.logger.info("%d document(s) indexed." % count)
        return True

    def get_bm(self, cluster):
        return cluster.buckets()

    def get_qim(self, cluster):
        return QueryIndexManager(cluster)

    def health(self, output=False, restrict=False):
        try:
            cluster = self.connect_s()
            result = cluster.ping()
            for endpoint, reports in result.endpoints.items():
                for report in reports:
                    if restrict and endpoint.value != 'kv':
                        continue
                    report_string = "{0}: {1} took {2} {3}".format(
                            endpoint.value,
                            report.remote,
                            report.latency,
                            report.state)
                    if output:
                        print(report_string)
                    self.logger.info(report_string)
                    if not report.state == PingState.OK and not output:
                        self.logger.error("Service %s not ok." % endpoint.value)
                        return False
            if output:
                diag_result = cluster.diagnostics()
                for endpoint, reports in diag_result.endpoints.items():
                    for report in reports:
                        report_string = "{0}: {1} last activity {2} {3}".format(
                            endpoint.value,
                            report.remote,
                            report.last_activity,
                            report.state)
                        print(report_string)
        except Exception as e:
            self.logger.error("Cluster ping failed: %s" % str(e))
            return False
        cluster.disconnect()
        return True

    def is_reachable(self):
        resolver = dns.resolver.Resolver()
        if self.tls:
            prefix = '_couchbases._tcp.'
        else:
            prefix = '_couchbase._tcp.'
        try:
            answer = resolver.resolve(prefix + self.hostname, "SRV")
            for srv in answer:
                record = {}
                record['hostname'] = str(srv.target).rstrip('.')
                host_answer = resolver.resolve(record['hostname'], 'A')
                record['address'] = host_answer[0].address
                self.srv_host_list.append(record)
            self.connect_name = self.srv_host_list[0]['hostname']
            self.rally_point_hostname = self.hostname
        except Exception:
            try:
                result = socket.gethostbyname(self.hostname)
            except Exception as e:
                self.logger.error("Can not resolve host %s: %s" % (self.hostname, str(e)))
                return False
            else:
                self.rally_point_hostname = self.hostname
                self.connect_name = self.hostname
        self.logger.info("is_reachable: rally_point_hostname: %s" % self.rally_point_hostname)
        self.logger.info("is_reachable: connect_name: %s" % self.connect_name)
        return True

    def get_memquota(self):
        return self.mem_quota

    def get_hostlist(self):
        host_list = []
        session = requests.Session()
        retries = Retry(total=60,
                        backoff_factor=0.1,
                        status_forcelist=[500, 501, 503])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))

        response = session.get(self.admin_url + '/pools/default',
                               auth=(self.username, self.password), verify=False, timeout=15)
        try:
            self.check_status_code(response.status_code)
        except Exception as e:
            self.logger.error("get_hostlist: %s" % str(e))
            raise

        response_json = json.loads(response.text)

        if 'memoryQuota' in response_json:
            self.mem_quota = response_json['memoryQuota']
        else:
            self.logger.error("get_hostlist: can not read memoryQuota.")
            raise Exception("Invalid response from host: can not get memory quota.")

        if 'nodes' not in response_json:
            self.logger.error("get_hostlist: error: invalid response from %s." % self.hostname)
            raise Exception("Can not get node list from %s." % self.hostname)
        for i in range(len(response_json['nodes'])):
            record = {}
            if 'alternateAddresses' in response_json['nodes'][i]:
                ext_host_name = response_json['nodes'][i]['alternateAddresses']['external']['hostname']
                record['external_name'] = ext_host_name
                record['external_ports'] = response_json['nodes'][i]['alternateAddresses']['external']['ports']
                self.logger.info("Added external node %s" % ext_host_name)
            host_name = response_json['nodes'][i]['configuredHostname']
            host_name = host_name.split(':')[0]
            record['host_name'] = host_name
            record['version'] = response_json['nodes'][i]['version']
            record['ostype'] = response_json['nodes'][i]['os']
            record['services'] = ','.join(response_json['nodes'][i]['services'])
            self.host_list.append(record)
            self.logger.info("Added node %s" % host_name)

        self.sw_version = self.host_list[0]['version']
        return True

    def print_host_map(self):
        ext_host_name = None
        ext_port_list = None
        i = 1
        if len(self.srv_host_list) > 0:
            print("Name %s is a domain with SRV records:" % self.rally_point_hostname)
            for record in self.srv_host_list:
                print(" => %s (%s)" % (record['hostname'], record['address']))
        print("Cluster Host List:")
        for record in self.host_list:
            if 'external_name' in record:
                ext_host_name = record['external_name']
            if 'external_ports' in record:
                ext_port_list = record['external_ports']
            host_name = record['host_name']
            version = record['version']
            ostype = record['ostype']
            services = record['services']
            print(" [%02d] %s" % (i, host_name), end=' ')
            if ext_host_name:
                print("[external]> %s" % ext_host_name, end=' ')
            if ext_port_list:
                for key in ext_port_list:
                    print("%s:%s" % (key, ext_port_list[key]), end=' ')
            print("[Services] %s [version] %s [platform] %s" % (services, version, ostype))
            i += 1
        for hostname in self.cluster_hosts():
            self.logger.info("cluster_hosts: %s" % hostname)
        self.logger.info("node_list: " + self.node_list())

    def get_version(self):
        return self.host_list[0]['version']

    def cluster_hosts(self):
        list = []
        for record in self.host_list:
            if 'external_name' in record:
                list.append(record['external_name'])
            else:
                list.append(record['host_name'])
        return iter(list)

    def node_list(self):
        list = []
        for record in self.host_list:
            if 'external_name' in record:
                list.append(record['external_name'])
            else:
                list.append(record['host_name'])
        return ','.join(list)

    def connect_s(self):
        self.logger.info("connect_s: connecting to: %s" % self.cb_string)
        retries = 0
        while True:
            try:
                cluster = couchbase.cluster.Cluster(self.cb_string, authenticator=self.auth, lockmode=LOCKMODE_NONE,
                                                    timeout_options=self.timeouts)
                return cluster
            except Exception as e:
                if retries == self.retries:
                    self.logger.error("connect_s: error: %s" % str(e))
                    raise Exception("Can not connect to cluster: %s" % str(e))
                else:
                    retries += 1
                    time.sleep(0.01 * retries)
                    continue

    def connect_bucket_s(self, cluster, bucket='default'):
        try:
            return cluster.bucket(bucket)
        except Exception as e:
            self.logger.error("connect_bucket_s: error: %s." % str(e))
            raise Exception("Can not connect to bucket: %s" % str(e))

    async def connect_a(self):
        self.logger.info("connect_a: connecting to: %s" % self.cb_string)
        retries = 0
        while True:
            try:
                cluster = acouchbase.cluster.Cluster(self.cb_string, authenticator=self.auth, lockmode=LOCKMODE_NONE,
                                                     timeout_options=self.timeouts)
                await cluster.on_connect()
                return cluster
            except Exception as e:
                if retries == self.retries:
                    self.logger.error("connect_a: error: %s" % str(e))
                    raise Exception("Can not connect to cluster: %s" % str(e))
                else:
                    retries += 1
                    time.sleep(0.01 * retries)
                    continue

    async def connect_bucket_a(self, cluster, bucket='default'):
        try:
            bucket = cluster.bucket(bucket)
            await bucket.on_connect()
            return bucket
        except Exception as e:
            self.logger.error("connect_bucket_a: error: %s." % str(e))
            raise Exception("Can not connect to bucket: %s" % str(e))

    def create_collection_s(self, bucket):
        try:
            return bucket.default_collection()
        except Exception as e:
            self.logger.error("collection_s: error: %s." % str(e))
            raise Exception("Can not connect to collection: %s" % str(e))

    async def create_collection_a(self, bucket):
        try:
            collection = bucket.default_collection()
            await collection.on_connect()
            return collection
        except Exception as e:
            self.logger.error("collection_a: error: %s." % str(e))
            raise Exception("Can not connect to collection: %s" % str(e))

    async def cb_get_a(self, collection, key):
        retries = 0
        while True:
            try:
                return await collection.get(key)
            except CouchbaseException as e:
                if retries == self.retries:
                    self.logger.error("cb_get_a: error: %s." % str(e))
                    raise Exception("Query error: %s" % str(e))
                else:
                    retries += 1
                    time.sleep(0.01 * retries)
                    continue

    def cb_get_s(self, collection, key):
        retries = 0
        while True:
            try:
                return collection.get(key)
            except CouchbaseException as e:
                if retries == self.retries:
                    self.logger.error("cb_get_s: error: %s." % str(e))
                    raise Exception("Query error: %s" % str(e))
                else:
                    retries += 1
                    time.sleep(0.01 * retries)
                    continue

    async def cb_upsert_a(self, collection, key, document):
        self.logger.info("cb_upsert_a %s" % key)
        retries = 0
        while True:
            try:
                self.logger.debug("cb_upsert_a entering loop")
                result = await collection.upsert(key, document)
                return result
            except Exception as e:
                if retries == self.retries:
                    self.logger.error("cb_upsert_a: error: %s." % str(e))
                    raise Exception("cb_upsert_a SDK error: %s" % str(e))
                else:
                    self.logger.debug("cb_upsert_a retry due to %s" % str(e))
                    retries += 1
                    time.sleep(0.01 * retries)
                    continue

    def cb_upsert_s(self, collection, key, document):
        retries = 0
        while True:
            try:
                result = collection.upsert(key, document)
                return result
            except Exception as e:
                if retries == self.retries:
                    self.logger.error("cb_upsert_s: error: %s." % str(e))
                    raise Exception("cb_upsert_s SDK error: %s" % str(e))
                else:
                    retries += 1
                    time.sleep(0.01 * retries)
                    continue

    async def cb_query_a(self, cluster, select, where, value, bucket="pillowfight"):
        contents = []
        retries = 0
        query = "SELECT " + select + " FROM " + bucket + " WHERE " + where + " = \"" + value + "\";"
        while True:
            try:
                result = cluster.query(query,
                                       QueryOptions(metrics=False, adhoc=True, pipeline_batch=128, max_parallelism=4,
                                                    pipeline_cap=1024, scan_cap=1024))
                async for item in result:
                    contents.append(item)
                return contents
            except ParsingFailedException as e:
                self.logger.error("cb_query_a: syntax error: %s", str(e))
                raise Exception("Query syntax error: %s" % str(e))
            except Exception as e:
                if retries == self.retries:
                    self.logger.error("cb_query_a: error: %s", str(e))
                    raise Exception("cb_query SDK error: %s" % str(e))
                else:
                    retries += 1
                    time.sleep(0.01 * retries)
                    continue

    def cb_query_s(self, cluster, select, where, value, bucket="pillowfight"):
        contents = []
        retries = 0
        query = "SELECT " + select + " FROM " + bucket + " WHERE " + where + " = \"" + value + "\";"
        while True:
            try:
                result = cluster.query(query,
                                       QueryOptions(metrics=False, adhoc=True, pipeline_batch=128, max_parallelism=4,
                                                    pipeline_cap=1024, scan_cap=1024))
                for item in result:
                    contents.append(item)
                return contents
            except ParsingFailedException as e:
                self.logger.error("cb_query_s: syntax error: %s", str(e))
                raise Exception("Query syntax error: %s" % str(e))
            except Exception as e:
                if retries == self.retries:
                    self.logger.error("cb_query_s: error: %s", str(e))
                    raise Exception("cb_query SDK error: %s" % str(e))
                else:
                    retries += 1
                    time.sleep(0.01 * retries)
                    continue

    def drop_bucket(self, bucket):
        cluster = self.connect_s()
        bm = self.get_bm(cluster)
        if self.is_bucket(bucket):
            try:
                bm.drop_bucket(bucket)
            except Exception as e:
                self.logger.error("drop_bucket: error: %s" % str(e))
                raise Exception("Could not drop bucket: %s" % str(e))
        cluster.disconnect()

    def drop_index(self, bucket, index):
        cluster = self.connect_s()
        self.logger.info("Dropping index %s.", index)
        queryText = 'DROP INDEX ' + index + ' ON ' + bucket + ' USING GSI;'
        if self.is_bucket(bucket) and self.is_index(bucket, index):
            try:
                result = cluster.query(queryText, QueryOptions(metrics=True))
            except Exception as e:
                self.logger.error("drop_index: error: %s" % str(e))
                raise Exception("Could not drop index: %s" % str(e))
            run_time = result.metadata().metrics().execution_time().microseconds
            run_time = run_time / 1000000
            self.logger.info("Index \"%s\" drop execution time: %f secs" % (index, run_time))
        cluster.disconnect()

    async def cb_remove_a(self, collection, key):
        self.logger.info("cb_upsert_a %s" % key)
        retries = 0
        while True:
            try:
                self.logger.debug("cb_upsert_a entering loop")
                result = await collection.remove(key)
                return result
            except DocumentNotFoundException:
                return None
            except Exception as e:
                if retries == self.retries:
                    self.logger.error("cb_remove_a: error: %s." % str(e))
                    raise Exception("cb_remove_a SDK error: %s" % str(e))
                else:
                    self.logger.debug("cb_upsert_a retry due to %s" % str(e))
                    retries += 1
                    time.sleep(0.01 * retries)
                    continue

    def cb_remove_s(self, collection, key):
        retries = 0
        while True:
            try:
                result = collection.remove(key)
                return result
            except DocumentNotFoundException:
                return None
            except Exception as e:
                if retries == self.retries:
                    self.logger.error("cb_remove_s: error: %s." % str(e))
                    raise Exception("cb_remove_s SDK error: %s" % str(e))
                else:
                    retries += 1
                    time.sleep(0.01 * retries)
                    continue

class params(object):

    def __init__(self):
        parser = argparse.ArgumentParser()
        parent_parser = argparse.ArgumentParser()
        parent_parser.add_argument('--user', action='store', help="User Name", default="Administrator")
        parent_parser.add_argument('--password', action='store', help="User Password", default="password")
        parent_parser.add_argument('--host', action='store', help="Cluster Node Name", default="localhost")
        parent_parser.add_argument('--bucket', action='store', help="Test Bucket", default="pillowfight")
        parent_parser.add_argument('--tls', action='store_true', help="Enable SSL")
        parent_parser.add_argument('--debug', action='store', help="Enable Debug Output", type=int, default=3)
        parent_parser.add_argument('--limit', action='store_true', help="Limited Network Connectivity")
        parent_parser.add_argument('--internal', action='store_true', help="Use Default over External Network")
        subparsers = parser.add_subparsers(dest='command')
        run_parser = subparsers.add_parser('run', help="Run Test Scenarios", parents=[parent_parser], add_help=False)
        list_parser = subparsers.add_parser('list', help="List Nodes", parents=[parent_parser], add_help=False)
        clean_parser = subparsers.add_parser('clean', help="Clean Up", parents=[parent_parser], add_help=False)
        health_parser = subparsers.add_parser('health', help="Cluster Health", parents=[parent_parser], add_help=False)
        run_parser.add_argument('--count', action='store', help="Record Count", type=int)
        run_parser.add_argument('--ops', action='store', help="Operation Count", type=int)
        run_parser.add_argument('--tload', action='store', help="Threads for Load", type=int)
        run_parser.add_argument('--trun', action='store', help="Threads for Run", type=int)
        run_parser.add_argument('--memquota', action='store', help="Bucket Memory Quota", type=int)
        run_parser.add_argument('--file', action='store', help="Input JSON File")
        run_parser.add_argument('--id', action='store', help="Numeric ID Field in JSON File", default="record_id")
        run_parser.add_argument('--query', action='store', help="Field to query in JSON File", default="last_name")
        run_parser.add_argument('--load', action='store_true', help="Only Load Data")
        run_parser.add_argument('--dryrun', action='store_true', help="Run Single Record Test Pass")
        run_parser.add_argument('--model', action='store_true', help="Run Calibration Style Test")
        run_parser.add_argument('--sync', action='store_true', help="Use Synchronous Connections")
        run_parser.add_argument('--clean', action='store_true', help="Run All Document Removal Test")
        run_parser.add_argument('--skipbucket', action='store_true', help="Use Preexisting bucket")
        self.parser = parser
        self.run_parser = run_parser
        self.list_parser = list_parser
        self.clean_parser = clean_parser
        self.health_parser = health_parser

class runPerformanceBenchmark(object):

    def __init__(self):
        self.cpu_count = os.cpu_count()
        self.telemetry_queue = multiprocessing.Queue()
        self.telemetry_return = multiprocessing.Queue()
        self.loadThreadCount = os.cpu_count() * 6
        self.runThreadCount = os.cpu_count() * 6
        self.maxRetries = 5
        self.recordId = 0
        self.currentOp = 0
        self.percentage = 0
        self.statusThreadRun = 1
        self.writePercent = 50
        self.replicaCount = 1
        self.keyArray = []
        self.hostList = []
        self.queryLatency = 1
        self.kvLatency = 1
        self.batchSize = 100
        self.queryBatchSize = 1
        self.clusterVersion = None
        self.bucketMemory = None
        self.inputFile = None
        self.idField = 'record_id'
        self.recordCount = 1000000
        self.operationCount = 100000
        self.dryRunFlag = False
        self.loadOnly = False
        self.runCpuModelFlag = False
        self.useSync = False
        self.skipBucket = False
        self.runRemoveTest = False
        self.limitNetworkPorts = False
        self.next_record = mpAtomicIncrement()
        self.errorCount = mpAtomicCounter()
        self.cbperfConfig = self.locateCfgFile()
        self.processConfigFile()
        input_data_size = 0

        print("CBPerf version %s" % VERSION)

        parms = params()
        parameters = parms.parser.parse_args()
        self.username = parameters.user
        self.password = parameters.password
        self.bucket = parameters.bucket
        self.host = parameters.host
        self.tls = parameters.tls
        self.debug = parameters.debug
        self.limitNetworkPorts = parameters.limit
        self.internalNetwork = parameters.internal
        self.fieldIndex = self.bucket + '_ix1'
        self.idIndex = self.bucket + '_id_ix1'

        logging.basicConfig()
        self.logger = logging.getLogger()
        couchbase.enable_logging()
        if self.debug == 0:
            self.logger.setLevel(logging.DEBUG)
        elif self.debug == 1:
            self.logger.setLevel(logging.INFO)
        elif self.debug == 2:
            self.logger.setLevel(logging.ERROR)
        else:
            self.logger.setLevel(logging.CRITICAL)

        if parameters.command == 'list':
            self.getHostList()
            sys.exit(0)

        if parameters.command == 'clean':
            self.cleanUp()
            sys.exit(0)

        if parameters.command == 'health':
            self.getHealth()
            sys.exit(0)

        if parameters.count:
            self.recordCount = parameters.count
        if parameters.ops:
            self.operationCount = parameters.ops
        if parameters.tload:
            self.loadThreadCount = parameters.tload
        if parameters.trun:
            self.runThreadCount = parameters.trun
        if parameters.memquota:
            self.bucketMemory = parameters.memquota
        if parameters.file:
            self.inputFile = parameters.file
        if parameters.id:
            self.idField = parameters.id
        if parameters.query:
            self.queryField = parameters.query
        if parameters.load:
            self.loadOnly = parameters.load
        if parameters.debug:
            self.debug = parameters.debug
        if parameters.dryrun:
            self.dryRunFlag = parameters.dryrun
        if parameters.model:
            self.runCpuModelFlag = parameters.model
        if parameters.sync:
            self.useSync = parameters.sync
            self.batchSize = 1
        if parameters.skipbucket:
            self.skipBucket = parameters.skipbucket
        if parameters.clean:
            self.runRemoveTest = parameters.clean

        if self.operationCount > self.recordCount:
            print("Error: Operation count must be equal or less than record count.")
            sys.exit(1)

        if not self.bucketMemory:
            one_mb = 1024 * 1024
            if self.inputFile:
                try:
                    input_data_size = os.path.getsize(self.inputFile)
                except Exception as e:
                    print("Can not get input file size: %s" % str(e))
                    sys.exit(1)
                total_data = input_data_size * self.recordCount * 1.2
                total_data_mb = round(total_data / one_mb)
            else:
                total_data = sys.getsizeof(DEFAULT_JSON) * self.recordCount * 1.2
                total_data_mb = round(total_data / one_mb)

            if total_data_mb < 256:
                total_data_mb = 256

            self.bucketMemory = total_data_mb

        if parameters.command == 'run':
            if self.dryRunFlag:
                self.dryRun()
                sys.exit(0)
            elif self.loadOnly:
                self.runTestScenario(self.loadSequence)
                sys.exit(0)
            elif self.runCpuModelFlag:
                self.runTestScenario(self.calibrateSequence)
                sys.exit(0)
            elif self.runRemoveTest:
                self.runTestScenario(self.removeSequence)
                sys.exit(0)
            else:
                print("Records   : %s" % f'{self.recordCount:,}')
                print("Operations: %s" % f'{self.operationCount:,}')
                self.runTestScenario(self.testSequence)
                sys.exit(0)

    def getInputJson(self):
        if self.inputFile:
            try:
                with open(self.inputFile, 'r') as inputFile:
                    inputJson = json.load(inputFile)
                inputFile.close()
                return inputJson
            except OSError as e:
                print("Can not read input file: %s" % str(e))
                sys.exit(1)
        else:
            return DEFAULT_JSON

    def locateCfgFile(self):
        if 'HOME' in os.environ:
            home_dir = os.environ['HOME']
        else:
            home_dir = '/var/tmp'

        if os.getenv('CBPERF_CONFIG'):
            config_file = os.getenv('CBPERF_CONFIG')
        elif os.path.exists("cbperf.cfg"):
            config_file = "cbperf.cfg"
        elif os.path.exists(home_dir + '/.cbperf/cbperf.cfg'):
            config_file = home_dir + '/.cbperf/cbperf.cfg'
        elif os.path.exists("/etc/cbperf/cbperf.cfg"):
            config_file = "/etc/cbperf/cbperf.cfg"
        else:
            config_file = home_dir + '/.cbperf/cbperf.cfg'

        return config_file

    def processConfigFile(self):
        if not os.path.exists(self.cbperfConfig):
            self.writeDefaultConfigFile()
        self.readConfigFile()

    def readConfigFile(self):
        config = configparser.ConfigParser()
        try:
            config.read(self.cbperfConfig)
        except Exception:
            print("Warning: Can not read config file %s" % self.cbperfConfig)
            self.logger.error("readConfigFile: can not read %s" % self.cbperfConfig)
            return False

        if config.has_section('settings'):
            if config.has_option('settings', 'operation_count'):
                self.operationCount = config.getint('settings', 'operation_count')
            if config.has_option('settings', 'record_count'):
                self.recordCount = config.getint('settings', 'record_count')
            if config.has_option('settings', 'kv_batch_size'):
                self.batchSize = config.getint('settings', 'kv_batch_size')
            if config.has_option('settings', 'query_batch_size'):
                self.queryBatchSize = config.getint('settings', 'query_batch_size')
            if config.has_option('settings', 'id_field'):
                self.idField = config.get('settings', 'id_field')
            if config.has_option('settings', 'query_latency'):
                self.queryLatency = config.getint('settings', 'query_latency')
            if config.has_option('settings', 'kv_latency'):
                self.kvLatency = config.getint('settings', 'kv_latency')

        if config.has_section('test_plan'):
            config_section = {}
            for (key, value) in config.items('test_plan'):
                value = eval(value)
                config_section[key] = {}
                config_section[key].update(value)
            self.testSequence = config_section

        if config.has_section('calibrate_plan'):
            config_section = {}
            for (key, value) in config.items('calibrate_plan'):
                value = eval(value)
                config_section[key] = {}
                config_section[key].update(value)
            self.calibrateSequence = config_section

        if config.has_section('load_plan'):
            config_section = {}
            for (key, value) in config.items('load_plan'):
                value = eval(value)
                config_section[key] = {}
                config_section[key].update(value)
            self.loadSequence = config_section

        if config.has_section('remove_plan'):
            config_section = {}
            for (key, value) in config.items('remove_plan'):
                value = eval(value)
                config_section[key] = {}
                config_section[key].update(value)
            self.removeSequence = config_section

    def writeDefaultConfigFile(self):
        config = configparser.ConfigParser()
        config_directory = os.path.dirname(self.cbperfConfig)
        testSequence = {
            'dataload': {
                'write': 100,
                'init': True,
                'run': True,
                'cleanup': False,
                'calibrate': False,
                'pause': True,
                'test': LOAD_DATA
            },
            'test1': {
                'write': 50,
                'init': False,
                'run': True,
                'cleanup': False,
                'calibrate': False,
                'pause': True,
                'test': KV_TEST
            },
            'test2': {
                'write': 5,
                'init': False,
                'run': True,
                'cleanup': False,
                'calibrate': False,
                'pause': True,
                'test': KV_TEST
            },
            'test3': {
                'write': 0,
                'init': False,
                'run': True,
                'cleanup': False,
                'calibrate': False,
                'pause': True,
                'test': KV_TEST
            },
            'test4': {
                'write': 50,
                'init': False,
                'run': True,
                'cleanup': False,
                'calibrate': False,
                'pause': True,
                'test': QUERY_TEST
            },
            'test5': {
                'write': 5,
                'init': False,
                'run': True,
                'cleanup': False,
                'calibrate': False,
                'pause': True,
                'test': QUERY_TEST
            },
            'test6': {
                'write': 0,
                'init': False,
                'run': True,
                'cleanup': True,
                'calibrate': False,
                'pause': False,
                'test': QUERY_TEST
            }
        }
        calibrateSequence = {
            'dataload': {
                'write': 100,
                'init': True,
                'run': True,
                'cleanup': False,
                'calibrate': False,
                'pause': True,
                'test': LOAD_DATA
            },
            'test1': {
                'write': 5,
                'init': False,
                'run': True,
                'cleanup': False,
                'calibrate': True,
                'pause': True,
                'test': QUERY_TEST
            },
            'test2': {
                'write': 0,
                'init': False,
                'run': True,
                'cleanup': True,
                'calibrate': True,
                'pause': False,
                'test': KV_TEST
            }
        }
        loadSequence = {
            'dataload': {
                'write': 100,
                'init': True,
                'run': True,
                'cleanup': False,
                'calibrate': False,
                'pause': False,
                'test': LOAD_DATA
            }
        }
        removeSequence = {
            'remove': {
                'write': 0,
                'init': False,
                'run': True,
                'cleanup': True,
                'calibrate': False,
                'pause': False,
                'test': REMOVE_DATA
            }
        }

        try:
            if not os.path.exists(config_directory):
                os.makedirs(config_directory)
        except Exception:
            self.logger.error("writeDefaultConfigFile: can not access config directory: %s" % config_directory)
            raise Exception("Can not access config file directory: %s" % str(e))

        config['settings'] = {'operation_count': '100000',
                              'record_count': '1000000',
                              'kv_batch_size': '100',
                              'query_batch_size': '1',
                              'id_field': 'record_id',
                              'query_latency': '1',
                              'kv_latency': '1',
                              }

        config['test_plan'] = testSequence
        config['calibrate_plan'] = calibrateSequence
        config['load_plan'] = loadSequence
        config['remove_plan'] = removeSequence

        try:
            with open(self.cbperfConfig, 'w') as configfile:
                config.write(configfile)
        except Exception as e:
            self.logger.error("writeDefaultConfigFile: %s" % str(e))
            raise Exception("Can not write config file: %s" % str(e))

    def runTestScenario(self, test_json):
        try:
            for key in test_json:
                print("Running scenario %s ..." % key)
                self.writePercent = test_json[key]['write']
                do_init = test_json[key]['init']
                do_run = test_json[key]['run']
                do_cleanup = test_json[key]['cleanup']
                do_pause = test_json[key]['pause']
                if test_json[key]['calibrate']:
                    self.runCalibration(test_json[key]['test'], init=do_init, run=do_run, cleanup=do_cleanup, pause=do_pause)
                else:
                    self.runTest(test_json[key]['test'], init=do_init, run=do_run, cleanup=do_cleanup, pause=do_pause)
        except KeyError as e:
            self.logger.error("runTestScenario: syntax error: %s" % str(e))
            raise Exception("Scenario syntax error: %s." % str(e))

    def pauseTestRun(self):
        document_index_count = self.replicaCount + 1
        try:
            self.logger.info("Connecting to cluster with host %s" % self.host)
            cb_cluster = cbutil(self.host, self.username, self.password, ssl=self.tls, internal=self.internalNetwork)
        except Exception as e:
            self.logger.critical("%s" % str(e))
            sys.exit(1)

        try:
            document_count = int(cb_cluster.bucket_count(self.bucket))
        except Exception as e:
            self.logger.error("pauseTestRun: %s" % str(e))
            raise Exception("%s" % str(e))

        print("Checking cluster health...", end=' ')
        if self.waitOn(cb_cluster.health, restrict=self.limitNetworkPorts):
            print("OK.")
        else:
            print("Not OK. Check cluster status.")
            self.logger.critical("pauseTestRun: cluster health check failed.")
            raise Exception("Cluster health check failed.")

        if not self.limitNetworkPorts:
            index_data = cb_cluster.index_stats(self.bucket)
            if self.idIndex not in index_data:
                self.logger.critical("Database is not properly indexed.")
                sys.exit(1)
            print("Waiting for %s document(s) to be indexed." % f'{document_count:,}')
            if not cb_cluster.index_wait(self.bucket, self.idIndex, document_count * document_index_count):
                sys.exit(1)
        else:
            print("Exposed port limit: skipping index check.")

    def cleanUp(self):
        try:
            self.logger.info("cleanUp: Connecting to cluster with host %s" % self.host)
            cb_cluster = cbutil(self.host, self.username, self.password, ssl=self.tls, internal=self.internalNetwork)
        except Exception as e:
            self.logger.critical("%s" % str(e))
            raise Exception("cleanUp: Can not connect to couchbase: %s" % str(e))

        print("Cleaning up.")
        print("Dropping index %s." % self.fieldIndex)
        cb_cluster.drop_index(self.bucket, self.fieldIndex)
        print("Dropping index %s." % self.idIndex)
        cb_cluster.drop_index(self.bucket, self.idIndex)
        if not self.skipBucket:
            print("Dropping bucket %s." % self.bucket)
            cb_cluster.drop_bucket(self.bucket)
        else:
            print("Leaving bucket in place.")

    def getHealth(self):
        try:
            self.logger.info("cleanUp: Connecting to cluster with host %s" % self.host)
            cb_cluster = cbutil(self.host, self.username, self.password, ssl=self.tls, internal=self.internalNetwork)
        except Exception as e:
            self.logger.critical("%s" % str(e))
            raise Exception("cleanUp: Can not connect to couchbase: %s" % str(e))

        cb_cluster.health(output=True, restrict=self.limitNetworkPorts)

    def getHostList(self):
        try:
            self.logger.info("Connecting to cluster with host %s" % self.host)
            cb_cluster = cbutil(self.host, self.username, self.password, ssl=self.tls, internal=self.internalNetwork)
            cb_cluster.print_host_map()
        except Exception as e:
            self.logger.critical("%s" % str(e))
            sys.exit(1)

    def waitOn(self, function, *args, **kwargs):
        count = 0
        while True:
            if count == self.maxRetries:
                return False
            if function(*args, **kwargs):
                return True
            else:
                count += 1
                time.sleep(0.2 * count)
                continue

    def modeString(self, mode):
        if mode == KV_TEST:
            mode_string = 'Key-Value Test'
        elif mode == QUERY_TEST:
            mode_string = 'Query Test'
        elif mode == LOAD_DATA:
            mode_string = 'Data Load'
        elif mode == REMOVE_DATA:
            mode_string = 'Remove Data'
        else:
            mode_string = 'Other Test'
        return mode_string

    def printStatusThread(self, count, threads):
        threadVector = [0 for i in range(threads)]
        totalTps = 0
        totalOps = 0
        entryOps = 0
        averageTps = 0
        maxTps = 0
        totalTime = 0
        averageTime = 0
        maxTime = 0
        sampleCount = 1
        time_per_record = 0
        trans_per_sec = 0
        start_shutdown = False
        debug_string = ""
        cycle = 1

        while True:
            entry = self.telemetry_queue.get()
            telemetry = entry.split(":")
            self.logger.debug(entry)
            if int(telemetry[0]) < RUN_STOP:
                entryOps = int(telemetry[1])
                time_delta = float(telemetry[2])
                reporting_thread = int(telemetry[0])
                threadVector[reporting_thread] = round(entryOps / time_delta)
                totalOps += entryOps
                trans_per_sec = sum(threadVector)
                op_time_delta = time_delta / entryOps
                totalTps = totalTps + trans_per_sec
                totalTime = totalTime + op_time_delta
                averageTps = totalTps / sampleCount
                averageTime = totalTime / sampleCount
                sampleCount += 1
                if trans_per_sec > maxTps:
                    maxTps = trans_per_sec
                if time_delta > maxTime:
                    maxTime = time_delta
                self.percentage = (totalOps / count) * 100
                if 'rss' in entry:
                    extra_string = "result count %d" % entry['rss']
                else:
                    extra_string = ""
                end_char = '\r'
                print("Operation %d of %d in progress, %.6f time, %d TPS, %d%% completed %s" %
                      (totalOps, count, op_time_delta, trans_per_sec, self.percentage, extra_string), end=end_char)
                self.logger.debug("%d %d %d %d %d %.6f %d" % (reporting_thread, entryOps, totalOps, totalTps, averageTps, averageTime, sampleCount))
            if int(telemetry[0]) == RUN_STOP:
                sys.stdout.write("\033[K")
                print("Operation %d of %d, %d%% complete." % (totalOps, count, self.percentage))
                print("Test Done.")
                print("%d average TPS." % averageTps)
                print("%d maximum TPS." % maxTps)
                print("%.6f average time." % averageTime)
                print("%.6f maximum time." % maxTime)
                return

    def runReset(self):
        self.currentOp = 0
        self.percentage = 0
        self.next_record.reset()
        self.telemetry_queue.close()
        self.telemetry_return.close()
        self.telemetry_queue = multiprocessing.Queue()
        self.telemetry_return = multiprocessing.Queue()

    def getMode(self):
        if self.useSync:
            return 'sync'
        else:
            return 'async'

    def dynamicStatusThread(self, latency=1):
        entry = ""
        threadVector = [0]
        return_telemetry = [0 for n in range(10)]
        threadVectorSize = 1
        totalTps = 0
        totalOps = 0
        totalCpu = 0
        averageTps = 0
        maxTps = 0
        maxTpsThreads = 0
        totalTime = 0
        averageTime = 0
        averageCpu = 0
        maxTime = 0
        sampleCount = 1
        loop_timeout = 5 * latency
        decTrend = False
        mem_usage = psutil.virtual_memory()
        tps_time_marker = time.perf_counter()
        loop_time_marker = tps_time_marker

        def exitFunction():
            return_telemetry[0] = RUN_STOP
            return_telemetry[1] = totalOps
            return_telemetry[2] = maxTime
            return_telemetry[3] = averageTime
            return_telemetry[4] = maxTps
            return_telemetry[5] = averageTps
            return_telemetry[6] = averageCpu
            return_telemetry[7] = mem_usage.percent
            return_telemetry[8] = decTrend
            return_telemetry[9] = maxTpsThreads
            return_telemetry_packet = ':'.join(str(i) for i in return_telemetry)
            self.telemetry_return.put(return_telemetry_packet)

        def threadVectorExtend(n):
            if len(threadVector) <= n:
                grow = (n - len(threadVector)) + 1
                threadVector.extend([0]*grow)

        while True:
            try:
                entry = self.telemetry_queue.get(block=False)
            except Empty:
                loop_time_check = time.perf_counter()
                loop_time_diff = loop_time_check - loop_time_marker
                if loop_time_diff > loop_timeout:
                    exitFunction()
                    return
                else:
                    continue
            telemetry = entry.split(":")
            self.logger.debug(entry)
            if int(telemetry[0]) < RUN_STOP:
                entryOps = int(telemetry[1])
                time_delta = float(telemetry[2])
                reporting_thread = int(telemetry[0])
                threadVectorExtend(reporting_thread)
                if reporting_thread >= threadVectorSize:
                    threadVectorSize = reporting_thread + 1
                threadVector[reporting_thread] = round(entryOps / time_delta)
                totalOps += entryOps
                trans_per_sec = sum(threadVector)
                op_time_delta = time_delta / entryOps
                totalTps = totalTps + trans_per_sec
                totalTime = totalTime + op_time_delta
                averageTps = totalTps / sampleCount
                averageTime = totalTime / sampleCount
                cpu_usage = psutil.cpu_percent()
                totalCpu = totalCpu + cpu_usage
                averageCpu = totalCpu / sampleCount
                mem_usage = psutil.virtual_memory()
                sampleCount += 1
                if trans_per_sec > maxTps:
                    maxTps = trans_per_sec
                    maxTpsThreads = threadVectorSize
                    tps_time_marker = time.perf_counter()
                else:
                    tps_check_time = time.perf_counter()
                    if (tps_check_time - tps_time_marker) > 120:
                        decTrend = True
                if time_delta > maxTime:
                    maxTime = time_delta
                end_char = '\r'
                print("Operation %d with %d threads, %.6f time, %d TPS, CPU %.1f%%, Mem %.1f    " %
                      (totalOps, threadVectorSize, op_time_delta, trans_per_sec, averageCpu, mem_usage.percent), end=end_char)
                self.logger.debug("%d %d %d %d %d %.6f %d" % (reporting_thread, entryOps, totalOps, totalTps, averageTps, averageTime, sampleCount))
                loop_time_marker = time.perf_counter()
                if decTrend or maxTime > latency or averageCpu > 90 or mem_usage.percent > 70:
                    exitFunction()
                    return
            if int(telemetry[0]) == RUN_STOP:
                exitFunction()
                return

    def runCalibration(self, mode=1, latency=1, init=True, run=True, cleanup=True, pause=False):
        loop = asyncio.get_event_loop()
        telemetry = [0 for n in range(3)]
        n = 0
        scale = []
        return_telemetry = []
        accelerator = 1

        print("Calibration module. Mode: %s" % self.modeString(mode))

        def emptyQueue():
            while True:
                try:
                    data = self.telemetry_queue.get(block=False)
                except Empty:
                    break

        try:
            self.logger.info("Connecting to cluster with host %s" % self.host)
            cb_cluster = cbutil(self.host, self.username, self.password, ssl=self.tls, internal=self.internalNetwork)

            if init:
                print("CBPerf calibrate (%s) connected to cluster %s version %s." % (self.getMode(),
                                                                                     self.host, cb_cluster.version))
                if not self.skipBucket:
                    print("Creating bucket %s." % self.bucket)
                    cb_cluster.create_bucket(self.bucket, self.bucketMemory)
                else:
                    print("Skipping bucket creation.")
                print("Creating index %s." % self.fieldIndex)
                cb_cluster.create_index(self.bucket, self.queryField, self.fieldIndex, self.replicaCount)
                print("Creating index %s." % self.idIndex)
                cb_cluster.create_index(self.bucket, self.idField, self.idIndex, self.replicaCount)
        except Exception as e:
            self.logger.critical("%s" % str(e))
            sys.exit(1)

        inputFileJson = self.getInputJson()

        if run:
            statusThread = multiprocessing.Process(target=self.dynamicStatusThread, args=(latency,))
            statusThread.daemon = True
            statusThread.start()

            print("Beginning calibration...")
            time_snap = time.perf_counter()
            start_time = time_snap
            while True:
                for i in range(accelerator):
                    scale.append(multiprocessing.Process(target=self.testInstance, args=(inputFileJson, mode, 0, n)))
                    scale[n].daemon = True
                    scale[n].start()
                    n += 1
                try:
                    entry = self.telemetry_return.get(block=False)
                    return_telemetry = entry.split(":")
                    if int(return_telemetry[0]) == RUN_STOP:
                        break
                except Empty:
                    pass
                if n >= INSTANCE_MAX:
                    telemetry[0] = RUN_STOP
                    telemetry_packet = ':'.join(str(i) for i in telemetry)
                    while True:
                        try:
                            self.telemetry_queue.put(telemetry_packet, block=False)
                            entry = self.telemetry_return.get(timeout=5)
                            return_telemetry = entry.split(":")
                            break
                        except Full:
                            emptyQueue()
                            continue
                        except Empty:
                            break
                    break
                time_check = time.perf_counter()
                time_diff = time_check - time_snap
                if time_diff >= 60:
                    time_snap = time.perf_counter()
                    accelerator *= 2
                time.sleep(5.0)

            for p in scale:
                p.terminate()
                p.join()

            emptyQueue()
            statusThread.join()
            end_time = time.perf_counter()

            sys.stdout.write("\033[K")
            print("Max threshold reached.")
            print(">> %d instances <<" % n)
            if len(return_telemetry) >= 10:
                print("=> %d total ops." % int(return_telemetry[1]))
                print("=> %.6f max time." % float(return_telemetry[2]))
                print("=> %.6f average time." % float(return_telemetry[3]))
                print("=> %d max TPS." % int(return_telemetry[4]))
                print("=> %.0f average TPS." % float(return_telemetry[5]))
                print("=> %.1f average CPU." % float(return_telemetry[6]))
                print("=> %.1f used memory." % float(return_telemetry[7]))
                print("=> Lag trend %s." % return_telemetry[8])
                print("=> Max TPS threads %d <<<" % int(return_telemetry[9]))
            else:
                print("Abnormal termination.")

            self.runReset()
            print("Calibration completed in %s" % time.strftime("%H hours %M minutes %S seconds.",
                                                         time.gmtime(end_time - start_time)))

        if pause:
            try:
                self.pauseTestRun()
            except Exception as e:
                print("Error: %s" % str(e))
                sys.exit(1)

        if cleanup:
            try:
                self.cleanUp()
            except Exception as e:
                print("Error: %s" % str(e))
                sys.exit(1)

    def dryRun(self):
        loop = asyncio.get_event_loop()
        # asyncio.set_event_loop(loop)
        record_number = 1
        retries = 0
        current_doc_count = 0

        print("Beginning dry run mode (%s)." % self.getMode())

        try:
            self.logger.info("Connecting to cluster with host %s" % self.host)
            cb_cluster = cbutil(self.host, self.username, self.password, ssl=self.tls, internal=self.internalNetwork)

            if not self.useSync:
                self.logger.info("Connecting to the cluster with async.")
                cluster = loop.run_until_complete(cb_cluster.connect_a())
            else:
                self.logger.info("Connecting to the cluster with sync.")
                cluster = cb_cluster.connect_s()

            if not self.skipBucket:
                print("Creating bucket %s." % self.bucket)
                cb_cluster.create_bucket(self.bucket, self.bucketMemory)
            else:
                print("Skipping bucket creation.")

            self.logger.info("Connecting to bucket.")
            if not self.useSync:
                bucket = loop.run_until_complete(cb_cluster.connect_bucket_a(cluster, self.bucket))
            else:
                bucket = cb_cluster.connect_bucket_s(cluster, self.bucket)

            self.logger.info("Connecting to collection.")
            if not self.useSync:
                collection = loop.run_until_complete(cb_cluster.create_collection_a(bucket))
            else:
                collection = cb_cluster.create_collection_s(bucket)

            print("Creating index %s." % self.fieldIndex)
            cb_cluster.create_index(self.bucket, self.queryField, self.fieldIndex, self.replicaCount)
            print("Creating index %s." % self.idIndex)
            cb_cluster.create_index(self.bucket, self.idField, self.idIndex, self.replicaCount)
        except Exception as e:
            self.logger.critical("%s" % str(e))
            sys.exit(1)

        print("CBPerf Test connected to cluster %s version %s." % (self.host, cb_cluster.version))

        inputFileJson = self.getInputJson()

        try:
            r = randomize()
            r.prepareTemplate(inputFileJson)
        except Exception as e:
            print("Can not load JSON template: %s." % str(e))
            sys.exit(1)

        record_id = str(format(record_number, '032'))

        if not self.limitNetworkPorts:
            index_data = cb_cluster.index_stats(self.bucket)
            if self.idIndex not in index_data:
                print("Database is not properly indexed.")

            current_doc_count = index_data[self.idIndex]['num_docs_indexed']

            if current_doc_count > 0:
                db_doc_count = int(current_doc_count) / (int(self.replicaCount) + 1)
                print("Warning: database not empty, %d doc(s) already indexed." % db_doc_count)
        else:
             print("Exposed port limit: Skipping index check.")

        print("Attempting to insert record %d..." % record_number)
        jsonDoc = r.processTemplate()
        jsonDoc[self.idField] = record_id
        if not self.useSync:
            result = loop.run_until_complete(cb_cluster.cb_upsert_a(collection, record_id, jsonDoc))
        else:
            result = cb_cluster.cb_upsert_s(collection, record_id, jsonDoc)

        print("Insert complete.")
        print(result.cas)

        print("Attempting to read record %d..." % record_number)
        if not self.useSync:
            result = loop.run_until_complete(cb_cluster.cb_get_a(collection, record_id))
        else:
            result = cb_cluster.cb_get_s(collection, record_id)

        print("Read complete.")
        print(json.dumps(result.content_as[dict], indent=2))

        if not self.limitNetworkPorts:
            print("Waiting for the inserted document to be indexed.")
            if not cb_cluster.index_wait(self.bucket, self.idIndex, current_doc_count + 1):
                sys.exit(1)
        else:
            print("Port limit: Skipping index wait.")
            time.sleep(0.1)

        while retries <= 5:
            print("Attempting to query record %d retry %d..." % (record_number, retries))
            if not self.useSync:
                result = loop.run_until_complete(cb_cluster.cb_query_a(cluster, self.queryField, self.idField, record_id, self.bucket))
            else:
                result = cb_cluster.cb_query_s(cluster, self.queryField, self.idField, record_id, self.bucket)

            if len(result) == 0:
                retries += 1
                print("No rows returned, retrying...")
                time.sleep(0.2 * retries)
                continue
            else:
                break

        if len(result) > 0:
            print("Query complete.")
            for i in range(len(result)):
                print(json.dumps(result[i], indent=2))
        else:
            print("Could not query record %d." % record_number)
            return

        if self.runRemoveTest:
            print("Attempting to remove record %d..." % record_number)
            if not self.useSync:
                result = loop.run_until_complete(cb_cluster.cb_remove_a(collection, record_id))
            else:
                result = cb_cluster.cb_remove_s(collection, record_id)

        print("Cleaning up.")
        print("Dropping index %s." % self.fieldIndex)
        cb_cluster.drop_index(self.bucket, self.fieldIndex)
        print("Dropping index %s." % self.idIndex)
        cb_cluster.drop_index(self.bucket, self.idIndex)
        if not self.skipBucket:
            print("Dropping bucket %s" % self.bucket)
            cb_cluster.drop_bucket(self.bucket)
        else:
            print("Leaving bucket in place.")

    def testCallBack(self, future):
        try:
            future.result()
        except Exception:
            self.errorCount.increment(1)

    def testInstance(self, json_block, mode=0, maximum=1, instance=1):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        telemetry = [0 for n in range(3)]
        tasks = []
        opSelect = rwMixer(self.writePercent)
        if mode == QUERY_TEST:
            runBatchSize = self.queryBatchSize
        else:
            runBatchSize = self.batchSize
        record_count = self.recordCount
        rand_gen = fastRandom(record_count)

        self.logger.debug("Starting test instance %d" % instance)

        try:
            self.logger.info("Connecting to cluster with host %s" % self.host)
            cb_cluster = cbutil(self.host, self.username, self.password, ssl=self.tls, internal=self.internalNetwork)

            self.logger.info("Connecting to the cluster.")
            if not self.useSync:
                cluster = loop.run_until_complete(cb_cluster.connect_a())
            else:
                cluster = cb_cluster.connect_s()

            self.logger.info("Connecting to bucket.")
            if not self.useSync:
                bucket = loop.run_until_complete(cb_cluster.connect_bucket_a(cluster, self.bucket))
            else:
                bucket = cb_cluster.connect_bucket_s(cluster, self.bucket)

            self.logger.info("Connecting to collection.")
            if not self.useSync:
                collection = loop.run_until_complete(cb_cluster.create_collection_a(bucket))
            else:
                collection = cb_cluster.create_collection_s(bucket)
        except Exception as e:
            self.logger.critical("%s" % str(e))
            sys.exit(1)

        try:
            r = randomize()
            r.prepareTemplate(json_block)
        except Exception as e:
            print("Can not load JSON template: %s." % str(e))
            sys.exit(1)

        self.logger.debug("Test instance %d connected, starting..." % instance)

        while True:
            tasks.clear()
            begin_time = time.time()
            for y in range(int(runBatchSize)):
                if maximum == 0:
                    record_number = rand_gen.value
                else:
                    record_number = self.next_record.next
                    if record_number > maximum:
                        break
                record_id = str(format(record_number, '032'))
                if opSelect.write(record_number):
                    jsonDoc = r.processTemplate()
                    jsonDoc[self.idField] = record_id
                    if not self.useSync:
                        tasks.append(cb_cluster.cb_upsert_a(collection, record_id, jsonDoc))
                    else:
                        result = cb_cluster.cb_upsert_s(collection, record_id, jsonDoc)
                        tasks.append(result)
                else:
                    if mode == REMOVE_DATA:
                        if not self.useSync:
                            tasks.append(cb_cluster.cb_remove_a(collection, record_id))
                        else:
                            result = cb_cluster.cb_remove_s(collection, record_id)
                            tasks.append(result)
                    elif mode == QUERY_TEST:
                        if not self.useSync:
                            tasks.append(cb_cluster.cb_query_a(cluster, self.queryField, self.idField, record_id, self.bucket))
                        else:
                            result = cb_cluster.cb_query_s(cluster, self.queryField, self.idField, record_id, self.bucket)
                            tasks.append(result)
                    else:
                        if not self.useSync:
                            tasks.append(cb_cluster.cb_get_a(collection, record_id))
                        else:
                            result = cb_cluster.cb_get_s(collection, record_id)
                            tasks.append(result)
            if len(tasks) > 0:
                if not self.useSync:
                    try:
                        result = loop.run_until_complete(asyncio.gather(*tasks))
                    except Exception as e:
                        print("testInstance: %s" % str(e))
                        sys.exit(1)
                end_time = time.time()
                loop_total_time = end_time - begin_time
                telemetry[0] = instance
                telemetry[1] = len(tasks)
                telemetry[2] = loop_total_time
                telemetry_packet = ':'.join(str(i) for i in telemetry)
                self.telemetry_queue.put(telemetry_packet)
            else:
                break

        self.logger.debug("Test thread %d complete, exiting." % instance)

    def runTest(self, mode=0, init=True, run=True, cleanup=True, pause=False):
        loop = asyncio.get_event_loop()
        telemetry = [0 for n in range(3)]

        print("Test module. Mode: %s" % self.modeString(mode))

        if mode == LOAD_DATA or mode == REMOVE_DATA:
            operation_count = int(self.recordCount)
            run_threads = int(self.loadThreadCount)
        else:
            operation_count = int(self.operationCount)
            run_threads = int(self.runThreadCount)

        try:
            self.logger.info("Connecting to cluster with host %s" % self.host)
            cb_cluster = cbutil(self.host, self.username, self.password, ssl=self.tls, internal=self.internalNetwork)

            if init:
                print("CBPerf test (%s) connected to %s cluster version %s." % (self.getMode(), self.host,
                                                                                cb_cluster.version))
                if not self.skipBucket:
                    print("Creating bucket %s." % self.bucket)
                    cb_cluster.create_bucket(self.bucket, self.bucketMemory)
                else:
                    print("Skipping bucket creation.")
                print("Creating index %s." % self.fieldIndex)
                cb_cluster.create_index(self.bucket, self.queryField, self.fieldIndex, self.replicaCount)
                print("Creating index %s." % self.idIndex)
                cb_cluster.create_index(self.bucket, self.idField, self.idIndex, self.replicaCount)
        except Exception as e:
            self.logger.critical("%s" % str(e))
            sys.exit(1)

        inputFileJson = self.getInputJson()

        if run:
            statusThread = multiprocessing.Process(target=self.printStatusThread, args=(operation_count, run_threads,))
            statusThread.daemon = True
            statusThread.start()

            if mode == REMOVE_DATA:
                read_percentage = 0
            else:
                read_percentage = 100 - self.writePercent
            print("Starting test with %s records - %d%% get, %d%% update"
                  % ('{:,}'.format(operation_count), read_percentage, self.writePercent))
            start_time = time.perf_counter()

            instances = [multiprocessing.Process(target=self.testInstance, args=(inputFileJson, mode, operation_count, n)) for n in range(run_threads)]
            for p in instances:
                p.daemon = True
                p.start()

            for p in instances:
                p.join()

            telemetry[0] = RUN_STOP
            telemetry_packet = ':'.join(str(i) for i in telemetry)
            self.telemetry_queue.put(telemetry_packet)
            statusThread.join()
            end_time = time.perf_counter()

            print("Test completed in %s" % time.strftime("%H hours %M minutes %S seconds.", time.gmtime(end_time - start_time)))
            self.runReset()

        if pause:
            try:
                self.pauseTestRun()
            except Exception as e:
                print("Error: %s" % str(e))
                sys.exit(1)

        if cleanup:
            try:
                self.cleanUp()
            except Exception as e:
                print("Error: %s" % str(e))
                sys.exit(1)

def main():
    runPerformanceBenchmark()

if __name__ == '__main__':

    try:
        main()
    except SystemExit as e:
        if e.code == 0:
            os._exit(0)
        else:
            os._exit(e.code)
