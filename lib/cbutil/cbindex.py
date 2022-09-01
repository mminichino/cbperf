##
##

from .exceptions import *
from .cbconnect import cb_connect
from .retries import retry_s
from .httpsessionmgr import api_session
from .httpexceptions import HTTPNotImplemented
import re


class cb_index(cb_connect):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.qim = None

    def index_name(self, name, field, index_name):
        field = field.replace('.', '_') if field else None
        field = re.sub('^_*', '', field) if field else None
        if index_name:
            index = index_name
        elif name != "_default" and field:
            index = name + '_' + field + '_ix'
        elif field:
            index = self.db.bucket_name + '_' + field + '_ix'
        else:
            index = '#primary'

        return index

    def index_lookup(self, name):
        if name == "_default":
            lookup = self.db.bucket_name
        else:
            lookup = name

        return lookup

    @retry_s(retry_count=10)
    def connect(self):
        try:
            self.connect_s()
        except Exception as err:
            raise IndexConnectError(f"can not connect to cluster: {err}")

    @retry_s(retry_count=10)
    def connect_bucket(self, name):
        try:
            self.bucket_s(name)
        except Exception as err:
            raise IndexBucketError("can not connect to bucket {}: {}".format(name, err))

    @retry_s(retry_count=10)
    def connect_scope(self, name="_default"):
        try:
            self.scope_s(name)
        except Exception as err:
            raise IndexScopeError("can not connect to scope {}: {}".format(name, err))

    @retry_s(retry_count=10)
    def connect_collection(self, name="_default"):
        try:
            self.collection_s(name)
        except Exception as err:
            raise IndexCollectionError("can not connect to collection {}: {}".format(name, err))

    @retry_s(retry_count=10)
    def is_index(self, field=None, name="_default", index_name=None):
        index = self.index_name(name, field, index_name)

        try:
            if name == "_default":
                collection_name = self.db.bucket_name
            else:
                collection_name = name
            indexList = self.db.qim.get_all_indexes(self.db.bucket_name)
            for i in range(len(indexList)):
                if index == '#primary':
                    if indexList[i].collection_name == collection_name and indexList[i].name == '#primary':
                        return True
                elif indexList[i].name == index:
                    return True
        except Exception as err:
            raise IndexStatError("Could not get index status: {}".format(err))

        return False

    @retry_s(retry_count=10, always_raise_list=(CollectionNameNotFound, IndexExistsError))
    def create_index(self, name="_default", field=None, index_name=None, replica=1):
        index = self.index_name(name, field, index_name)

        try:
            keyspace = self.db.keyspace_s(name)
            if field and index != '#primary':
                queryText = 'CREATE INDEX ' + index + ' ON ' + keyspace + '(' + field + ') WITH {"num_replica": ' + str(replica) + '};'
            else:
                queryText = 'CREATE PRIMARY INDEX ON ' + keyspace + ' WITH {"num_replica": ' + str(replica) + '};'
            result = self.cb_query_s(sql=queryText)
            return result
        except CollectionNameNotFound:
            raise
        except IndexExistsError:
            return True
        except Exception as err:
            raise IndexQueryError("can not create index on {}: {}".format(name, err))

    @retry_s(retry_count=10, always_raise_list=(CollectionNameNotFound,))
    def drop_index(self, name="_default", field=None, index_name=None):
        index = self.index_name(name, field, index_name)

        try:
            keyspace = self.db.keyspace_s(name)
            if field and index != '#primary':
                queryText = 'DROP INDEX ' + index + ' ON ' + keyspace + ' USING GSI;'
            else:
                queryText = 'DROP PRIMARY INDEX ON ' + keyspace + ' USING GSI;'
            result = self.cb_query_s(sql=queryText)
            return result
        except CollectionNameNotFound:
            raise
        except Exception as err:
            raise IndexQueryError("can not drop index on {}: {}".format(name, err))

    @retry_s(retry_count=10, factor=0.5, allow_list=(IndexNotReady,))
    def index_wait(self, name="_default", field=None, index_name=None):
        index = self.index_name(name, field, index_name)
        lookup = self.index_lookup(name)
        record_count = self.collection_count_s(name)

        if not self.node_api_accessible:
            try:
                self.alt_index_check(name=name, field=field, index_name=index_name, check_count=record_count)
            except Exception:
                raise IndexNotReady(f"alt check index {index} not ready")
        else:
            index_stats = self.index_stats()
            for key in index_stats:
                if key == lookup:
                    for item in index_stats[key]:
                        if item == index:
                            pending = index_stats[key][item]['num_docs_pending']
                            queued = index_stats[key][item]['num_docs_queued']
                            count = index_stats[key][item]['items_count']
                            if (pending != 0 and queued != 0) or count < record_count:
                                raise IndexNotReady("{} not ready, count {} pending {} queued {}".format(
                                    index, count, pending, queued))

    def index_stats(self, name=None):
        if not name:
            bucket = self.db.bucket_name
        else:
            bucket = name

        index_data = {}
        endpoint = '/api/v1/stats/' + bucket
        s = api_session(self.username, self.password)
        for node in self.all_hosts:
            s.set_host(node, self.ssl, self.node_port)
            try:
                response_json = s.api_get(endpoint)
            except HTTPNotImplemented:
                continue
            for key in response_json:
                index_name = key.split(':')[-1]
                index_object = key.split(':')[-2]
                if index_object not in index_data:
                    index_data[index_object] = {}
                if index_name not in index_data[index_object]:
                    index_data[index_object][index_name] = response_json[key]

        return index_data

    def get_index_key(self, name="_default", field=None, index_name=None):
        index = self.index_name(name, field, index_name)
        lookup = self.index_lookup(name)
        query_text = 'SELECT * FROM system:indexes ;'
        doc_key_field = 'meta().id'

        result_index = self.cb_query_s(sql=query_text)

        for row in result_index:
            for key, value in row.items():
                if value['name'] == index and value['keyspace_id'] == lookup:
                    if len(value['index_key']) == 0:
                        return doc_key_field
                    else:
                        return value['index_key'][0]

        raise IndexNotFoundError(f"index {index} not found")

    def alt_index_check(self, name="_default", field=None, index_name=None, check_count=0):
        index = self.index_name(name, field, index_name)
        keyspace = self.db.keyspace_s(name)

        try:
            query_field = self.get_index_key(name, field, index_name)
        except Exception:
            raise

        query_text = f"SELECT {query_field} FROM {keyspace} WHERE TOSTRING({query_field}) LIKE \"%\" ;"
        result = self.cb_query_s(sql=query_text)

        if len(result) >= check_count and len(result) > 0:
            return True
        else:
            raise IndexNotReady(f"index {index} not ready")

    @retry_s(retry_count=10, factor=0.5, allow_list=(IndexNotReady,))
    def index_online(self, keyspace_id, field=None, primary=False):
        if primary:
            query_text = f"SELECT * FROM system:indexes AS i where i.keyspace_id = \"{keyspace_id}\" and i.is_primary = true;"
        elif field is not None:
            query_text = f"SELECT * FROM system:indexes AS i UNNEST i.index_key AS l where i.keyspace_id = \"{keyspace_id}\" and l = \"`{field}`\";"
        else:
            raise IndexInternalError(f"index_online: either set primary to True or provide field parameter")

        try:
            result = self.cb_query_s(sql=query_text)
            for row in result:
                for key, value in row.items():
                    if key != 'i':
                        continue
                    if value['keyspace_id'] == keyspace_id and value['state'] == 'online':
                        return True
            raise IndexNotReady(f"index not online")
        except Exception as err:
            raise IndexNotReady(f"index_online: keyspace_id {keyspace_id} error: {err}")

    @retry_s(retry_count=10, factor=0.5, allow_list=(IndexNotReady,))
    def index_list(self, bucket):
        query_text = "SELECT * FROM system:indexes;"
        index_list = {}

        try:
            result = self.cb_query_s(sql=query_text)
            for row in result:
                for key, value in row.items():
                    if 'bucket_id' in value:
                        if value['bucket_id'] == bucket:
                            index_list[value['id']] = value['name']
                    else:
                        if value['keyspace_id'] == bucket:
                            index_list[value['id']] = value['name']
            return index_list
        except Exception as err:
            raise IndexNotReady(f"index_list: bucket {bucket} error: {err}")

    @retry_s(retry_count=10, factor=0.5, allow_list=(IndexNotReady,))
    def delete_wait(self, field=None, name="_default", index_name=None):
        if self.is_index(field=field, name=name, index_name=index_name):
            raise IndexNotReady(f"delete_wait: index on {name} still exists")
