#!/usr/bin/env python3

import warnings
from lib.randomize import (rand_init, rand_gender, past_date, dob_date, rand_first_name, rand_last_name, month_value, credit_card, social_security_number, four_digits, zip_code,
                           account_number, dollar_amount, numeric_sequence, hash_code, address_line, rand_city, rand_state, nick_name, email_address, user_name, phone_number,
                           boolean_value, date_code, year_value, past_date_slash, past_date_hyphen, past_date_text, dob_slash, dob_hyphen, dob_text, day_value, rand_franchise,
                           rand_corporation)

warnings.filterwarnings("ignore")


def test_random_1():
    rand_init()
    g = rand_gender()
    _past_date = past_date()
    _dob_date = dob_date()
    first_name = rand_first_name(g)
    last_name = rand_last_name()
    month = month_value()
    print("Credit Card: " + credit_card())
    print("SSN        : " + social_security_number())
    print("Four Digits: " + four_digits())
    print("ZIP Code   : " + zip_code())
    print("Account    : " + account_number())
    print("Dollar     : " + dollar_amount())
    print("Sequence   : " + numeric_sequence())
    print("Hash       : " + hash_code())
    print("Address    : " + address_line())
    print("City       : " + rand_city())
    print("State      : " + rand_state())
    print("First      : " + first_name)
    print("Last       : " + last_name)
    print("Nickname   : " + nick_name(first_name, last_name))
    print("Email      : " + email_address(first_name, last_name))
    print("Username   : " + user_name(first_name, last_name))
    print("Phone      : " + phone_number())
    print("Boolean    : " + str(boolean_value()))
    print("Date       : " + date_code())
    print("Year       : " + year_value())
    print("Month      : " + month)
    print("Day        : " + day_value(month))
    print("Franchise  : " + rand_franchise())
    print("Corporation: " + rand_corporation())
    print("Past Date 1: " + past_date_slash(_past_date))
    print("Past Date 2: " + past_date_hyphen(_past_date))
    print("Past Date 3: " + past_date_text(_past_date))
    print("DOB Date 1 : " + dob_slash(_dob_date))
    print("DOB Date 2 : " + dob_hyphen(_dob_date))
    print("DOB Date 3 : " + dob_text(_dob_date))
