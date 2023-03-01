##
##

import os
import warnings
import json
import multiprocessing
import random
import re
import io
from jinja2 import Template
from jinja2.environment import Environment
from jinja2.runtime import DebugUndefined
from jinja2.meta import find_undeclared_variables
from datetime import datetime, timedelta
from lib.exceptions import ConfigFileError
import base64
import hashlib
from enum import Enum
import numpy
from PIL import Image

warnings.filterwarnings("ignore")
lib_dir = os.path.dirname(os.path.realpath(__file__))
package_dir = os.path.dirname(lib_dir)


class HashMode(Enum):
    sha1 = 0
    sha256 = 1
    sha512 = 2
    md5 = 3


class Gender(Enum):
    M = 0
    F = 1


class FastRandom(object):

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


class MPAtomicIncrement(object):

    def __init__(self, i=1, s=1):
        self.count = multiprocessing.Value('i', i)
        self._set_size = s
        self.set_count = multiprocessing.Value('i', s)

    def reset(self, i=1):
        with self.count.get_lock():
            self.count.value = i

    def set_size(self, n):
        self._set_size = n
        with self.set_count.get_lock():
            self.set_count.value = self._set_size

    @property
    def do_increment(self):
        with self.set_count.get_lock():
            if self.set_count.value == 1:
                self.set_count.value = self._set_size
                return True
            else:
                self.set_count.value -= 1
                return False

    @property
    def next(self):
        if self.do_increment:
            with self.count.get_lock():
                current = self.count.value
                self.count.value += 1
            return current
        else:
            return self.count.value


data_file_name = package_dir + '/config/data.json'
data_struct = {}
issued_struct = {
    "ssn": {},
    "username": {},
    "email": {},
    "nickname": {}
}
requested_tags = None
template = None
compiled: Template
password_hash = HashMode.sha1.value
incrementor = MPAtomicIncrement()
incrementor_block = MPAtomicIncrement(s=10)


def load_data() -> None:
    global data_struct

    try:
        with open(data_file_name, 'r') as data_file:
            data_struct = json.load(data_file)
    except Exception as err:
        raise ConfigFileError(f"can not read random data file: {err}")


def random_number_seq(n):
    min_lc = ord(b'0')
    len_lc = 10
    ba = bytearray(random.getrandbits(8) for i in range(n))
    for i, b in enumerate(ba):
        ba[i] = min_lc + b % len_lc
    return ba.decode('utf-8')


def random_match(match: re.Match):
    if match.re.pattern == 'N':
        return random_number(1, m=2)
    else:
        return random_number_seq(1)


def random_number(n, m=0):
    while True:
        min_lc = ord(b'0')
        len_lc = 10
        ba = bytearray(random.getrandbits(8) for i in range(n))
        for i, b in enumerate(ba):
            ba[i] = min_lc + b % len_lc
        v = int(ba.decode('utf-8'))
        if v < m:
            continue
        return str(v)


def random_number_range(minimum, maximum):
    max_b = int(maximum).bit_length()
    while True:
        n = random.getrandbits(max_b)
        if minimum <= n <= maximum:
            return str(n)


def random_string_lower(n):
    min_lc = ord(b'a')
    len_lc = 26
    ba = bytearray(random.getrandbits(8) for i in range(n))
    for i, b in enumerate(ba):
        ba[i] = min_lc + b % len_lc
    return ba.decode('utf-8')


def random_string_upper(n):
    min_lc = ord(b'A')
    len_lc = 26
    ba = bytearray(random.getrandbits(8) for i in range(n))
    for i, b in enumerate(ba):
        ba[i] = min_lc + b % len_lc
    return ba.decode('utf-8')


def random_hash(n):
    ba = bytearray(random.getrandbits(8) for i in range(n))
    for i, b in enumerate(ba):
        min_lc = ord(b'0') if b < 85 else ord(b'A') if b < 170 else ord(b'a')
        len_lc = 10 if b < 85 else 26
        ba[i] = min_lc + b % len_lc
    return ba.decode('utf-8')


def random_bits(n, m=0):
    while True:
        d = random.getrandbits(n)
        if d >= m:
            break
    yield d


def month_number():
    return random_number_range(1, 12)


def month_day(month):
    if int(month) in [1, 3, 5, 7, 8, 10, 12]:
        return random_number_range(1, 31)
    elif int(month) in [4, 6, 9, 11]:
        return random_number_range(1, 30)
    else:
        return random_number_range(1, 28)


def year_number():
    value = int(random_number_seq(2)) + 1920
    return value


def credit_card():
    data = data_struct.get('card_masks')
    if not data:
        raise ConfigFileError("No credit card mask data")
    rand_gen = FastRandom(len(data), 0)
    card_mask = data[rand_gen.value]
    return re.sub('X', random_match, card_mask)


def social_security_number():
    while True:
        issued = '-'.join([random_number_seq(3), random_number_seq(2), random_number_seq(4)])
        if not issued_struct['ssn'].get(issued):
            issued_struct['ssn'].update({issued: None})
            return issued


def three_digits():
    return random_number_seq(3)


def four_digits():
    return random_number_seq(4)


def zip_code():
    return random_number_seq(5)


def account_number():
    return random_number_seq(10)


def numeric_sequence():
    return random_number_seq(16)


def dollar_amount():
    value = random.getrandbits(8) % 5 + 1
    return random_number(value, m=1) + '.' + random_number_seq(2)


def boolean_value():
    if random.getrandbits(1) == 1:
        return True
    else:
        return False


def year_value():
    return str(year_number())


def month_value():
    value = month_number()
    return f'{int(value):02}'


def day_value(month):
    value = month_day(month)
    return f'{int(value):02}'


def past_date():
    _past_date = datetime.today() - timedelta(days=random.getrandbits(12))
    return _past_date


def dob_date():
    _past_date = datetime.today() - timedelta(days=random.getrandbits(14), weeks=1040)
    return _past_date


def past_date_slash(_past_date):
    return _past_date.strftime("%m/%d/%Y")


def past_date_hyphen(_past_date):
    return _past_date.strftime("%m-%d-%Y")


def past_date_text(_past_date):
    return _past_date.strftime("%b %d %Y")


def dob_slash(_past_date):
    return _past_date.strftime("%m/%d/%Y")


def dob_hyphen(_past_date):
    return _past_date.strftime("%m-%d-%Y")


def dob_text(_past_date):
    return _past_date.strftime("%b %d %Y")


def hash_code():
    return random_hash(16)


def rand_gender():
    return Gender(next(random_bits(1)))


def rand_street_name():
    data = data_struct.get('street_names')
    if not data:
        raise ConfigFileError("No random street name data")
    rand_gen = FastRandom(len(data), 0)
    return data[rand_gen.value]


def rand_street_suffix():
    data = data_struct.get('street_suffix')
    if not data:
        raise ConfigFileError("No random street suffix data")
    rand_gen = FastRandom(len(data), 0)
    return data[rand_gen.value]


def rand_first_name(g: Gender):
    if g == Gender.M:
        data = data_struct.get('first_names', {}).get('male')
    else:
        data = data_struct.get('first_names', {}).get('female')
    if not data:
        raise ConfigFileError("No random first name data")
    rand_gen = FastRandom(len(data), 0)
    return data[rand_gen.value]


def rand_last_name():
    data = data_struct.get('last_names')
    if not data:
        raise ConfigFileError("No random last name data")
    rand_gen = FastRandom(len(data), 0)
    return data[rand_gen.value]


def rand_city():
    data = data_struct.get('city_names')
    if not data:
        raise ConfigFileError("No random city name data")
    rand_gen = FastRandom(len(data), 0)
    return data[rand_gen.value]


def rand_state():
    data = data_struct.get('state_names_short')
    if not data:
        raise ConfigFileError("No random state name data")
    rand_gen = FastRandom(len(data), 0)
    return data[rand_gen.value]


def address_line():
    return ' '.join([random_number(4, m=1), rand_street_name(), rand_street_suffix()])


def phone_number():
    data = data_struct.get('area_codes')
    if not data:
        raise ConfigFileError("No random street suffix data")
    rand_gen = FastRandom(len(data), 0)
    area_code = data[rand_gen.value]
    nxx = re.sub('N', random_match, 'NXX')
    nxx = re.sub('X', random_match, nxx)
    return '-'.join([area_code, nxx, random_number_seq(4)])


def date_code():
    now_time = datetime.now()
    datetime_str = now_time.strftime("%Y-%m-%d %H:%M:%S")
    return datetime_str


def nick_name(first_name="John", last_name="Doe"):
    name = first_name[0].lower() + last_name.lower()
    found = [issued_struct['nickname'][key] for key in issued_struct['nickname'] if re.match(f"^{name}", key)]
    if len(found) > 0:
        suffix = str(len(found))
        name += suffix
    return name


def email_address(first_name="John", last_name="Doe"):
    mailbox = first_name.lower() + '.' + last_name.lower()
    found = [issued_struct['email'][key] for key in issued_struct['email'] if re.match(f"^{mailbox}", key)]
    if len(found) > 0:
        suffix = str(len(found))
        mailbox += suffix
    return mailbox + '@example.com'


def user_name(first_name="John", last_name="Doe"):
    user = first_name.lower() + last_name.lower()
    found = [issued_struct['username'][key] for key in issued_struct['username'] if re.match(f"^{user}", key)]
    if len(found) > 0:
        suffix = str(len(found))
        user += suffix
    return user


def rand_image():
    random_matrix = numpy.random.rand(128, 128, 3) * 255
    im = Image.fromarray(random_matrix.astype('uint8')).convert('RGBA')
    with io.BytesIO() as output:
        im.save(output, format="JPEG2000")
        contents = output.getvalue()
    encoded = base64.b64encode(contents)
    encoded = encoded.decode('utf-8')
    return encoded


def rand_password():
    password = "password"
    if password_hash == HashMode.sha1.value:
        digest = hashlib.sha1(password.encode('utf-8')).digest()
    elif password_hash == HashMode.sha256.value:
        digest = hashlib.sha256(password.encode('utf-8')).digest()
    elif password_hash == HashMode.sha512.value:
        digest = hashlib.sha512(password.encode('utf-8')).digest()
    else:
        digest = hashlib.md5(password.encode('utf-8')).digest()
    return base64.b64encode(digest).decode('utf-8')


def rand_init():
    load_data()


def prepare_template(json_block):
    global requested_tags, template, compiled
    block_string = json.dumps(json_block)
    env = Environment(undefined=DebugUndefined)
    t = env.from_string(block_string)
    rendered = t.render()
    ast = env.parse(rendered)
    requested_tags = find_undeclared_variables(ast)
    template = block_string
    compiled = Template(template)


def process_template():
    global compiled
    g = rand_gender()
    first_name = rand_first_name(g)
    last_name = rand_last_name()
    _past_date = past_date()
    _dob_date = dob_date()
    month = month_number()
    random_image = None

    if 'rand_image' in requested_tags:
        random_image = rand_image()

    formatted_block = compiled.render(date_time=date_code(),
                                      incr_value=incrementor.next,
                                      incr_block=incrementor_block.next,
                                      rand_credit_card=credit_card(),
                                      rand_ssn=social_security_number(),
                                      rand_four=four_digits(),
                                      rand_account=account_number(),
                                      rand_id=numeric_sequence(),
                                      rand_zip_code=zip_code(),
                                      rand_dollar=dollar_amount(),
                                      rand_hash=hash_code(),
                                      rand_address=address_line(),
                                      rand_city=rand_city(),
                                      rand_state=rand_state(),
                                      rand_first=first_name,
                                      rand_last=last_name,
                                      rand_nickname=nick_name(first_name, last_name),
                                      rand_email=email_address(first_name, last_name),
                                      rand_username=user_name(first_name, last_name),
                                      rand_phone=phone_number(),
                                      rand_bool=boolean_value(),
                                      rand_year=year_value(),
                                      rand_month=month,
                                      rand_day=day_value(month),
                                      rand_date_1=past_date_slash(_past_date),
                                      rand_date_2=past_date_hyphen(_past_date),
                                      rand_date_3=past_date_text(_past_date),
                                      rand_dob_1=dob_slash(_dob_date),
                                      rand_dob_2=dob_hyphen(_dob_date),
                                      rand_dob_3=dob_text(_dob_date),
                                      rand_image=random_image,
                                      rand_password=rand_password(),
                                      )
    finished = formatted_block.encode('ascii')
    json_block = json.loads(finished)
    return json_block
