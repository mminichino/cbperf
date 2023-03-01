#!/usr/bin/env python3

import argparse
import json


class Params(object):

    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--male', action='store', help="First Name")
        parser.add_argument('--female', action='store', help="First Name")
        parser.add_argument('--last', action='store', help="Last Name")
        parser.add_argument('--street', action='store', help="Street Names")
        parser.add_argument('--city', action='store', help="City Names")
        parser.add_argument('--cards', action='store', help="Credit Card Masks")
        parser.add_argument('--area', action='store', help="Area Codes")
        self.args = parser.parse_args()

    @property
    def parameters(self):
        return self.args


p = Params()
options = p.parameters
male_names = []
female_names = []
last_names = []
street_names = []
city_names = []
card_masks = []
area_codes = []


with open(options.male, 'r') as dat_file:
    while True:
        line = dat_file.readline()
        if not line:
            break
        item = line.strip()
        male_names.append(item)

with open(options.female, 'r') as dat_file:
    while True:
        line = dat_file.readline()
        if not line:
            break
        item = line.strip()
        female_names.append(item)

with open(options.last, 'r') as dat_file:
    while True:
        line = dat_file.readline()
        if not line:
            break
        item = line.strip()
        last_names.append(item)

with open(options.street, 'r') as dat_file:
    while True:
        line = dat_file.readline()
        if not line:
            break
        item = line.strip()
        street_names.append(item)

with open(options.city, 'r') as dat_file:
    while True:
        line = dat_file.readline()
        if not line:
            break
        item = line.strip()
        city_names.append(item)

with open(options.cards, 'r') as dat_file:
    while True:
        line = dat_file.readline()
        if not line:
            break
        item = line.strip()
        card_masks.append(item)

with open(options.area, 'r') as dat_file:
    while True:
        line = dat_file.readline()
        if not line:
            break
        item = line.strip()
        area_codes.append(item)

data_struct = {
    "first_names": {
        "male": male_names,
        "female": female_names
    },
    "last_names": last_names,
    "street_names": street_names,
    "city_names": city_names,
    "card_masks": card_masks,
    "area_codes": area_codes,
    "street_suffix": [
            'Alley',
            'Annex',
            'Arcade',
            'Avenue',
            'Bayou',
            'Boulevard',
            'Bypass',
            'Causeway',
            'Circle',
            'Court',
            'Drive',
            'Expressway',
            'Freeway',
            'Highway',
            'Junction',
            'Loop',
            'Motorway',
            'Parkway',
            'Pike',
            'Place',
            'Plaza',
            'Road',
            'Skyway',
            'Spur',
            'Street',
            'Throughway',
            'Trail',
            'Turnpike',
            'Way'
        ],
    "state_names_short": [
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
        ],
    "state_names_long": [
            'Alabama',
            'Alaska',
            'Arizona',
            'Arkansas',
            'California',
            'Colorado',
            'Connecticut',
            'Delaware',
            'Florida',
            'Georgia',
            'Hawaii',
            'Idaho',
            'Illinois',
            'Indiana',
            'Iowa',
            'Kansas',
            'Kentucky',
            'Louisiana',
            'Maine',
            'Maryland',
            'Massachusetts',
            'Michigan',
            'Minnesota',
            'Mississippi',
            'Missouri',
            'Montana',
            'Nebraska',
            'Nevada',
            'New Hampshire',
            'New Jersey',
            'New Mexico',
            'New York',
            'North Carolina',
            'North Dakota',
            'Ohio',
            'Oklahoma',
            'Oregon',
            'Pennsylvania',
            'Rhode Island',
            'South Carolina',
            'South Dakota',
            'Tennessee',
            'Texas',
            'Utah',
            'Vermont',
            'Virginia',
            'Washington',
            'West Virginia',
            'Wisconsin',
            'Wyoming'
    ]
}

with open('data.json', 'w') as data_file:
    json.dump(data_struct, data_file)
