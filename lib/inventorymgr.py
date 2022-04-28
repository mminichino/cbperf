##
##

import traceback
import json
import re
from json.decoder import JSONDecodeError
import jinja2
from jinja2.meta import find_undeclared_variables
from distutils.util import strtobool
from .exceptions import *
from lib.cbutil.cbdebug import cb_debug
from lib.cbutil.randomize import randomize


class schemaElement(object):

    def __init__(self, name):
        self.name = name
        self.buckets = []
        self.rules = []


class bucketElement(object):

    def __init__(self, name):
        self.name = name
        self.scopes = []


class scopeElement(object):

    def __init__(self, name):
        self.name = name
        self.collections = []


class collectionElement(object):

    def __init__(self, name, bucket, scope):
        self.name = name
        self.bucket = bucket
        self.scope = scope
        self.id = None
        self.primary_key = False
        self.override_count = False
        self.record_count = None
        self.batch_size = None
        self.default_batch_size = None
        self.size = None
        if name == '_default':
            self.key_prefix = bucket
        else:
            self.key_prefix = name
        self.schema_variable = None
        self.schema = {}
        self.indexes = []


class inventoryManager(object):

    def __init__(self, inventory, args, by_reference=False):
        debugger = cb_debug(self.__class__.__name__)
        self.logger = debugger.logger
        self.inventory_json = inventory
        self.args = args
        self.file_schema_json = None
        self.schemas = []

        if not hasattr(args, 'schema'):
            return

        try:
            for w, schema_object in enumerate(self.inventory_json['inventory']):
                for key, value in schema_object.items():
                    if key != self.args.schema:
                        continue

                    self.logger.info(f"adding schema {key} to inventory")
                    node = schemaElement(key)
                    self.schemas.insert(0, node)

                    for x, bucket in enumerate(value['buckets']):
                        bucket_name = self.resolve_variables(bucket['name'])
                        if len(bucket_name) == 0:
                            raise InventoryConfigError(f"schema {key}: bucket name can not be null")

                        self.logger.info(f"adding bucket {bucket_name} to inventory")
                        node = bucketElement(bucket_name)
                        self.schemas[0].buckets.insert(0, node)

                        for y, scope in enumerate(value['buckets'][x]['scopes']):
                            self.logger.info("adding scope %s to inventory" % scope['name'])
                            node = scopeElement(scope['name'])
                            self.schemas[0].buckets[0].scopes.insert(0, node)

                            for z, collection in enumerate(value['buckets'][x]['scopes'][y]['collections']):
                                id_key_name = self.resolve_variables(collection['idkey'])
                                if len(id_key_name) == 0:
                                    raise InventoryConfigError(f"schema {key}: id field can not be null")

                                self.logger.info("adding collection %s to inventory" % collection['name'])
                                node = collectionElement(collection['name'], bucket_name, scope['name'])
                                self.schemas[0].buckets[0].scopes[0].collections.insert(0, node)
                                self.schemas[0].buckets[0].scopes[0].collections[0].id = id_key_name

                                if by_reference:
                                    self.schemas[0].buckets[0].scopes[0].collections[0].schema.update(eval(collection['schema']))
                                else:
                                    schema_data_dict = collection['schema']

                                    if len(schema_data_dict) == 0:
                                        raise InventoryConfigError(f"schema {key}: document definition can not be null")
                                    elif type(schema_data_dict) == str:
                                        try:
                                            schema_file_name = self.resolve_variables(schema_data_dict)
                                            schema_data_dict = self.read_input_file(schema_file_name)
                                        except Exception as err:
                                            InventoryConfigError(f"document JSON error: {err}")

                                    if type(schema_data_dict) != dict:
                                        raise InventoryConfigError(f"schema {key}: document definition must be JSON")

                                    self.schemas[0].buckets[0].scopes[0].collections[0].schema.update(schema_data_dict)

                                    doc_size = self.get_document_size(schema_data_dict, id_key_name)
                                    self.schemas[0].buckets[0].scopes[0].collections[0].size = doc_size

                                self.schemas[0].buckets[0].scopes[0].collections[0].primary_index = collection['primary_index']
                                self.schemas[0].buckets[0].scopes[0].collections[0].override_count = collection['override_count']

                                if 'record_count' in collection:
                                    self.schemas[0].buckets[0].scopes[0].collections[0].record_count = collection['record_count']

                                if 'batch_size' in collection:
                                    self.schemas[0].buckets[0].scopes[0].collections[0].batch_size = collection['batch_size']
                                    self.schemas[0].buckets[0].scopes[0].collections[0].default_batch_size = collection['batch_size']

                                if 'indexes' in collection:
                                    for index_field in collection['indexes']:
                                        index_field_name = self.resolve_variables(index_field)
                                        if len(index_field_name) == 0:
                                            raise InventoryConfigError(f"schema {key}: index name can not be null")

                                        self.logger.info(f"adding index for field {index_field_name} to inventory")
                                        index_data = {}
                                        index_data['field'] = index_field_name
                                        index_data['name'] = self.indexName(self.schemas[0].buckets[0].scopes[0].collections[0], index_field_name)
                                        self.schemas[0].buckets[0].scopes[0].collections[0].indexes.append(index_data)

                    if 'rules' in value:
                        for r, rule in enumerate(value['rules']):
                            self.logger.info("adding rule %s to inventory" % rule['name'])
                            self.schemas[0].rules.append(rule)
        except Exception as err:
            print(traceback.format_exc())
            raise InventoryConfigError("inventory syntax error: {}".format(err))

    def get_document_size(self, document, key):
        try:
            r = randomize()
            r.prepareTemplate(document)
            doc_template = r.processTemplate()
            doc_template[key] = 1
            size = len(json.dumps(doc_template))
            return size
        except Exception as err:
            raise InventoryConfigError(f"can not process document template: {err}")

    def read_input_file(self, filename):
        try:
            with open(filename, 'r') as input_file:
                schema_json = json.load(input_file)
            input_file.close()
            return schema_json
        except OSError as err:
            raise InventoryConfigError(f"can not read input file {filename}: {err}")
        except JSONDecodeError as err:
            raise InventoryConfigError(f"invalid JSON data in input file {filename}: {err}")

    def resolve_variables(self, value):
        raw_template = jinja2.Template(value)
        formatted_value = raw_template.render(
            FILE_PARAMETER=self.args.file if self.args.file else "",
            ID_FIELD_PARAMETER=self.args.id if self.args.id else "",
            BUCKET_PARAMETER=self.args.bucket if self.args.bucket else "",
        )
        return formatted_value

    @property
    def schemaList(self):
        for w, schema_object in enumerate(self.inventory_json['inventory']):
            for key, value in schema_object.items():
                yield key

    def getSchema(self, schema):
        return next((s for s in self.schemas if s.name == schema), None)

    def nextBucket(self, schema):
        for i in range(len(schema.buckets)):
            yield schema.buckets[i]

    def hasRules(self, schema):
        if len(schema.rules) > 0:
            return True
        else:
            return False

    def nextRule(self, schema):
        yield next((r for r in schema.rules), None)

    def nextScope(self, bucket):
        for i in range(len(bucket.scopes)):
            yield bucket.scopes[i]

    def nextCollection(self, scope):
        for i in range(len(scope.collections)):
            yield scope.collections[i]

    def hasIndexes(self, collection):
        if len(collection.indexes) > 0:
            return True
        else:
            return False

    def hasPrimaryIndex(self, collection):
        if type(collection.primary_index) != bool:
            return bool(strtobool(collection.primary_index))
        else:
            return collection.primary_index

    def indexName(self, collection, field):
        field = field.replace('.', '_')
        field = re.sub('^_*', '', field)

        if collection.name != '_default':
            index = collection.name + '_' + field + '_ix'
        else:
            index = collection.bucket + '_' + field + '_ix'

        return index

    def nextIndex(self, collection):
        for i in range(len(collection.indexes)):
            yield collection.indexes[i]['field'], collection.indexes[i]['name']

    def getIndex(self, collection, field):
        return next(((i['field'], i['name']) for i in collection.indexes if i['field'] == field), None)

    def overrideRecordCount(self, collection):
        if type(collection.override_count) != bool:
            return bool(strtobool(collection.override_count))
        else:
            return collection.override_count

    def getRecordCount(self, collection):
        return int(collection.record_count)
