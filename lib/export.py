##
##

import logging
import sys
import pandas as pd
from lib.exceptions import ExportException
from lib.cbutil.cbsync import cb_connect_s
import lib.config as config


class CBExport(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

        try:
            self.db = cb_connect_s(config.host,
                                   config.username,
                                   config.password,
                                   ssl=config.tls,
                                   external=config.external_network).init()
        except Exception as err:
            raise ExportException(f"can not connect to Couchbase: {err}")

    def as_csv(self):
        for bucket in config.schema.buckets:
            self.db.bucket(bucket.name)
            bucket_stats = self.db.bucket_stats(bucket.name)

            for scope in bucket.scopes:
                self.db.scope(scope.name)

                for collection in scope.collections:
                    data = []
                    count = 0

                    self.db.collection(collection.name)
                    print(f"Processing collection {self.db.keyspace}")
                    output_file = f"{config.output_dir}/{str(self.db.keyspace).replace('.','-')}.csv"

                    for i in range(1, bucket_stats['itemCount']):
                        result = self.db.cb_get(i)
                        if not result:
                            break
                        data.append(result)
                        count += 1
                        print(f" = Document {count}", end='\r')

                    sys.stdout.write("\033[K")
                    print(f" == Retrieved {count} records")
                    print(f" == Creating {output_file}")
                    df = pd.json_normalize(data)
                    df.to_csv(output_file, encoding='utf-8', index=False)
