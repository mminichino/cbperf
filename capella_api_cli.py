#!/usr/bin/env -S python3 -W ignore

import argparse
import json
from lib.cbutil.capsessionmgr import capella_session

parser = argparse.ArgumentParser()
parser.add_argument('-e', action='store')
parser.add_argument('-g', action='store_true')
args = parser.parse_args()

capella = capella_session()

if not args.e:
    raise Exception("Please specify an endpoint.")

endpoint = args.e
result = None

if args.g:
    result = capella.api_get(endpoint)

result_formatted = json.dumps(result, indent=2)
print(result_formatted)
