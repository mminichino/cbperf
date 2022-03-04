#!/usr/bin/env -S python3 -W ignore

'''
Couchbase Performance Utility - create schema JSON files
'''

import os
import sys
import json

DEFAULT_SCHEMA_INVENTORY = {
    'inventory': [
        {
            'default': {
                'buckets': [
                    {
                        'name': 'cbperf',
                        'scopes': [
                            {
                                'name': '_default',
                                'collections': [
                                    {
                                        'name': '_default',
                                        'schema': 'DEFAULT_JSON',
                                        'idkey': 'record_id',
                                        'primary_index': False,
                                        'indexes': [
                                            'record_id',
                                            'last_name',
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                ]
            }
        },
        {
            'profile_demo': {
                'buckets': [
                    {
                        'name': 'sample_app',
                        'scopes': [
                            {
                                'name': 'profiles',
                                'collections': [
                                    {
                                        'name': 'user_data',
                                        'schema': 'USER_PROFILE_JSON',
                                        'idkey': 'record_id',
                                        'primary_index': True,
                                        'indexes': [
                                            'record_id',
                                            'nickname',
                                            'user_id',
                                        ]
                                    },
                                    {
                                        'name': 'user_images',
                                        'schema': 'USER_IMAGE_JSON',
                                        'idkey': 'record_id',
                                        'primary_index': True,
                                        'indexes': [
                                            'record_id',
                                        ]
                                    }
                                ]
                            },
                        ]
                    },
                ],
                'rules': [
                    {
                        'name': 'rule0',
                        'type': 'link',
                        'foreign_key': 'sample_app:profiles:user_data:picture',
                        'primary_key': 'sample_app:profiles:user_images:record_id',
                    },
                ]
            }
        }
    ]
}

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

USER_PROFILE_JSON = {
    'record_id': 'record_id',
    'name': '{{ rand_first }} {{ rand_last }}',
    'nickname': '{{ rand_nickname }}',
    'picture': 'link_user_images',
    'user_id': '{{ rand_username }}',
    'email': '{{ rand_email }}',
    'email_verified': '{{ rand_bool }}',
    'first_name': '{{ rand_first }}',
    'last_name': '{{ rand_last }}',
}

USER_IMAGE_JSON = {
    'record_id': 'record_id',
    'image': '{{ rand_image }}',
}

class generateFiles(object):

    def __init__(self):
        output_file = 'schema.json'
        self.schema_json = DEFAULT_SCHEMA_INVENTORY

        try:
            with open(output_file, 'w') as configfile:
                self.traverseJson()
                json.dump(self.schema_json, configfile, indent=2)
                configfile.write("\n")
                configfile.close()
        except Exception as e:
            print("Error: Can not write config files: %s" % str(e))
            sys.exit(1)

    def traverseJson(self):
        for w, schema_object in enumerate(self.schema_json['inventory']):
            for key, value in schema_object.items():
                for x, bucket in enumerate(value['buckets']):
                    for y, scope in enumerate(value['buckets'][x]['scopes']):
                        for z, collection in enumerate(value['buckets'][x]['scopes'][y]['collections']):
                            self.schema_json['inventory'][w][key]['buckets'][x]['scopes'][y]['collections'][z]['schema'] = eval(collection['schema'])


def main():
    generateFiles()


if __name__ == '__main__':

    try:
        main()
    except SystemExit as e:
        if e.code == 0:
            os._exit(0)
        else:
            os._exit(e.code)
