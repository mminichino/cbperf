##
##

import traceback
import json
import jinja2
from jinja2.meta import find_undeclared_variables
from distutils.util import strtobool
from .exceptions import *
from lib.cbutil.cbdebug import cb_debug


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

        try:
            for w, schema_object in enumerate(self.inventory_json['inventory']):
                for key, value in schema_object.items():
                    if key != self.args.schema:
                        continue
                    self.logger.info(f"adding schema {key} to inventory")
                    if key == 'external_file':
                        value, self.file_schema_json = self.resolve_variables(json.dumps(value))
                    else:
                        self.file_schema_json = None
                    node = schemaElement(key)
                    self.schemas.insert(0, node)
                    for x, bucket in enumerate(value['buckets']):
                        self.logger.info("adding bucket %s to inventory" % bucket['name'])
                        node = bucketElement(bucket['name'])
                        self.schemas[0].buckets.insert(0, node)
                        for y, scope in enumerate(value['buckets'][x]['scopes']):
                            self.logger.info("adding scope %s to inventory" % scope['name'])
                            node = scopeElement(scope['name'])
                            self.schemas[0].buckets[0].scopes.insert(0, node)
                            for z, collection in enumerate(value['buckets'][x]['scopes'][y]['collections']):
                                self.logger.info("adding collection %s to inventory" % collection['name'])
                                node = collectionElement(collection['name'], bucket['name'], scope['name'])
                                self.schemas[0].buckets[0].scopes[0].collections.insert(0, node)
                                if by_reference:
                                    self.schemas[0].buckets[0].scopes[0].collections[0].schema.update(
                                        eval(collection['schema']))
                                else:
                                    if self.file_schema_json:
                                        self.schemas[0].buckets[0].scopes[0].collections[0].schema.update(self.file_schema_json)
                                    else:
                                        self.schemas[0].buckets[0].scopes[0].collections[0].schema.update(collection['schema'])
                                self.schemas[0].buckets[0].scopes[0].collections[0].id = collection['idkey']
                                self.schemas[0].buckets[0].scopes[0].collections[0].primary_index = collection['primary_index']
                                self.schemas[0].buckets[0].scopes[0].collections[0].override_count = collection['override_count']
                                if 'record_count' in collection:
                                    self.schemas[0].buckets[0].scopes[0].collections[0].record_count = collection['record_count']
                                if 'batch_size' in collection:
                                    self.schemas[0].buckets[0].scopes[0].collections[0].batch_size = collection['batch_size']
                                if 'indexes' in collection:
                                    for index_field in collection['indexes']:
                                        self.logger.info("adding index for field %s to inventory" % index_field)
                                        index_data = {}
                                        index_data['field'] = index_field
                                        index_data['name'] = self.indexName(
                                            self.schemas[0].buckets[0].scopes[0].collections[0], index_field)
                                        self.schemas[0].buckets[0].scopes[0].collections[0].indexes.append(index_data)
                    if 'rules' in value:
                        for r, rule in enumerate(value['rules']):
                            self.logger.info("adding rule %s to inventory" % rule['name'])
                            self.schemas[0].rules.append(rule)
        except Exception as err:
            print(traceback.format_exc())
            raise InventoryConfigError("inventory syntax error: {}".format(err))

    def resolve_variables(self, value):
        schema_json = {}

        env = jinja2.Environment(undefined=jinja2.DebugUndefined)
        template = env.from_string(value)
        rendered = template.render()
        ast = env.parse(rendered)
        requested_vars = find_undeclared_variables(ast)

        for variable in requested_vars:
            if variable == 'FILE_PARAMETER':
                if not self.args.file:
                    raise ParameterError("schema requested FILE_PARAMETER but parameter file was not supplied.")
                try:
                    with open(self.args.file, 'r') as input_file:
                        schema_json = json.load(input_file)
                    input_file.close()
                except OSError as err:
                    raise ParameterError(f"can not read input file {self.args.file}: {err}")
            elif variable == 'ID_FIELD_PARAMETER':
                if not self.args.id:
                    raise ParameterError("schema requested ID_FIELD_PARAMETER but parameter id was not supplied.")
            elif variable == 'BUCKET_PARAMETER':
                if not self.args.bucket:
                    raise ParameterError("schema requested ID_FIELD_PARAMETER but parameter id was not supplied.")

        raw_template = jinja2.Template(value)
        formatted_value = raw_template.render(
            ID_FIELD_PARAMETER=self.args.id,
            BUCKET_PARAMETER=self.args.bucket,
        )

        new_schema_json = json.loads(formatted_value)
        return new_schema_json, schema_json

    @property
    def schemaList(self):
        for s in self.schemas:
            yield s.name

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
        if collection.scope != '_default':
            scope_text = '_' + collection.scope
        else:
            scope_text = ''
        if collection.name != '_default':
            collection_text = '_' + collection.name
        else:
            collection_text = ''
        field = field.replace('.', '_')
        return collection.bucket + scope_text + collection_text + '_' + field + '_ix'

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
