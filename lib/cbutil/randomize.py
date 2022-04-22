##
##

import random
from datetime import datetime, timedelta
import numpy
from PIL import Image
import io
import base64
import json
from jinja2 import Template
from jinja2.environment import Environment
from jinja2.runtime import DebugUndefined
from jinja2.meta import find_undeclared_variables


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
    def booleanValue(self):
        if random.getrandbits(1) == 1:
            return True
        else:
            return False

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
    def pastDate(self):
        past_date = datetime.today() - timedelta(days=random.getrandbits(12))
        return past_date

    @property
    def dobDate(self):
        past_date = datetime.today() - timedelta(days=random.getrandbits(14), weeks=1040)
        return past_date

    def pastDateSlash(self, past_date):
        return past_date.strftime("%m/%d/%Y")

    def pastDateHyphen(self, past_date):
        return past_date.strftime("%m-%d-%Y")

    def pastDateText(self, past_date):
        return past_date.strftime("%b %d %Y")

    def dobSlash(self, past_date):
        return past_date.strftime("%m/%d/%Y")

    def dobHyphen(self, past_date):
        return past_date.strftime("%m-%d-%Y")

    def dobText(self, past_date):
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
        return '-'.join([self._randomNumber(3), '555', self._randomNumber(4)])

    @property
    def dateCode(self):
        return self.datetimestr

    def nickName(self, first_name="John", last_name="Doe"):
        return first_name[0].lower() + last_name.lower()

    def emailAddress(self, first_name="John", last_name="Doe"):
        return first_name.lower() + '.' + last_name.lower() + '@example.com'

    def userName(self, first_name="John", last_name="Doe"):
        return first_name.lower() + last_name.lower() + self.fourDigits

    def randImage(self):
        random_matrix = numpy.random.rand(128, 128, 3) * 255
        im = Image.fromarray(random_matrix.astype('uint8')).convert('RGBA')
        with io.BytesIO() as output:
            im.save(output, format="JPEG2000")
            contents = output.getvalue()
        encoded = base64.b64encode(contents)
        encoded = encoded.decode('utf-8')
        return encoded

    def testAll(self):
        past_date = self.pastDate
        dob_date = self.dobDate
        first_name = self.firstName
        last_name = self.lastName
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
        print("First      : " + first_name)
        print("Last       : " + last_name)
        print("Nickname   : " + self.nickName(first_name, last_name))
        print("Email      : " + self.emailAddress(first_name, last_name))
        print("Username   : " + self.userName(first_name, last_name))
        print("Phone      : " + self.phoneNumber)
        print("Boolean    : " + str(self.booleanValue))
        print("Date       : " + self.dateCode)
        print("Year       : " + self.yearValue)
        print("Month      : " + self.monthValue)
        print("Day        : " + self.dayValue)
        print("Past Date 1: " + self.pastDateSlash(past_date))
        print("Past Date 2: " + self.pastDateHyphen(past_date))
        print("Past Date 3: " + self.pastDateText(past_date))
        print("DOB Date 1 : " + self.dobSlash(dob_date))
        print("DOB Date 2 : " + self.dobHyphen(dob_date))
        print("DOB Date 3 : " + self.dobText(dob_date))

    def prepareTemplate(self, json_block):
        block_string = json.dumps(json_block)
        env = Environment(undefined=DebugUndefined)
        template = env.from_string(block_string)
        rendered = template.render()
        ast = env.parse(rendered)
        self.requested_tags = find_undeclared_variables(ast)
        self.template = block_string
        self.compiled = Template(self.template)

    def processTemplate(self):
        first_name = self.firstName
        last_name = self.lastName
        past_date = self.pastDate
        dob_date = self.dobDate
        random_image = None

        if 'rand_image' in self.requested_tags:
            random_image = self.randImage()

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
                                              rand_first=first_name,
                                              rand_last=last_name,
                                              rand_nickname=self.nickName(first_name, last_name),
                                              rand_email=self.emailAddress(first_name, last_name),
                                              rand_username=self.userName(first_name, last_name),
                                              rand_phone=self.phoneNumber,
                                              rand_bool=self.booleanValue,
                                              rand_year=self.yearValue,
                                              rand_month=self.monthValue,
                                              rand_day=self.dayValue,
                                              rand_date_1=self.pastDateSlash(past_date),
                                              rand_date_2=self.pastDateHyphen(past_date),
                                              rand_date_3=self.pastDateText(past_date),
                                              rand_dob_1=self.dobSlash(dob_date),
                                              rand_dob_2=self.dobHyphen(dob_date),
                                              rand_dob_3=self.dobText(dob_date),
                                              rand_image=random_image,
                                              )
        finished = formattedBlock.encode('ascii')
        jsonBlock = json.loads(finished)
        return jsonBlock
