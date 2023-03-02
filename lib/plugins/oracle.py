##
##

import os
import logging
import lib.config as config
import oracledb
from lib.exceptions import DriverError


class DBDriver(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

        if 'ORACLE_SID' in os.environ:
            self.oracle_sid = os.environ['ORACLE_SID']
        else:
            raise DriverError("Please set ORACLE_SID environment variable")

        try:
            self.db = oracledb.connect(user=config.username, password=config.password, host=config.host, port=1521, service_name=self.oracle_sid)
        except Exception as err:
            raise DriverError(f"con not connect to database: {err}")

    def get_table_names(self):
        with self.db.cursor() as cursor:
            for row in cursor.execute(r"SELECT table_name FROM user_tables"):
                print(row)
