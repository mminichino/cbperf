##
##

import os
import re
import logging
import oracledb
from lib.exceptions import DriverError
from lib.plugins.relational import Schema, Table, Column


class DBDriver(object):

    def __init__(self, plugin_vars: dict):
        self.logger = logging.getLogger(self.__class__.__name__)

        if 'connect' in plugin_vars:
            self.logger.debug(f"connect data: {plugin_vars.get('connect')}")
            vector = re.split('[@/:]', plugin_vars.get('connect'))
            self.username = vector[0]
            self.password = vector[1] if len(vector) > 1 else None
            self.hostname = vector[2] if len(vector) > 2 else None
            self.oracle_sid = vector[3] if len(vector) > 3 else None

        if 'ORACLE_SID' in os.environ:
            self.oracle_sid = os.environ['ORACLE_SID']
        elif not self.oracle_sid:
            raise DriverError("Please set ORACLE_SID environment variable")
        else:
            os.environ['ORACLE_SID'] = self.oracle_sid

        if None in (self.username, self.password, self.hostname, self.oracle_sid):
            raise DriverError(f"Please supply required connection parameters")

        try:
            self.db = oracledb.connect(user=self.username, password=self.password, host=self.hostname, port=1521, service_name=self.oracle_sid)
            self.logger.info(f"Connected to database version {self.db.version}")
        except Exception as err:
            raise DriverError(f"con not connect to database: {err}")

    def get_schema(self):
        schema = Schema.build()
        for table_data in self.get_table_rows():
            table_size = self.get_table_size(table_data['table_name'])
            table = Table.build(table_data['table_name'], table_size['table_mib'], int(table_data['num_rows']))
            for field_data in self.get_row_fields(table_data['table_name']):
                if re.match(r"interval year.* to month", field_data['data_type']):
                    field_select = f"to_char({field_data['column_name']})"
                else:
                    field_select = field_data['column_name']
                field = Column.add(field_data['column_name'], field_data['data_type'], field_select)
                table.add(field)
            schema.add(table)
        return schema

    def get_table_size(self, table_name: str):
        with self.db.cursor() as cursor:
            cursor.execute(f"""
                select segment_name table_name,
                sum(bytes)/(1024*1024) table_mib
                from user_extents
                where segment_type='TABLE'
                and segment_name = '{table_name.upper()}'
                group by segment_name""")
            columns = [c[0].lower() for c in cursor.description]
            results = cursor.fetchall()
            return dict(zip(columns, results[0]))

    def get_table_rows(self):
        with self.db.cursor() as cursor:
            cursor.execute("select table_name, num_rows from user_tables")
            columns = [c[0].lower() for c in cursor.description]
            results = cursor.fetchall()
            for result in results:
                fields = tuple(str(f).lower() for f in result)
                yield dict(zip(columns, fields))

    def get_table_indexes(self, table_name: str):
        with self.db.cursor() as cursor:
            cursor.execute(f"""
                select ic.index_name, ic.column_name, ie.column_expression
                from all_ind_columns ic left join all_ind_expressions ie
                on ie.index_owner = ic.index_owner
                and ie.index_name = ic.index_name
                and ie.column_position = ic.column_position
                where ic.table_name = '{table_name.upper()}'""")
            columns = [c[0].lower() for c in cursor.description]
            results = cursor.fetchall()
            column_list = list(map(lambda c: c[1] if not c[2] else c[2], [result for result in results]))
            column_list = list(map(lambda c: re.sub('"', '', c).lower(), [column for column in column_list]))
            return column_list

    def get_row_fields(self, table_name: str):
        with self.db.cursor() as cursor:
            cursor.execute(f"select column_name, data_type from all_tab_columns where table_name = '{table_name.upper()}'")
            columns = [c[0].lower() for c in cursor.description]
            results = cursor.fetchall()
            for result in results:
                fields = tuple(f.lower() for f in result)
                yield dict(zip(columns, fields))

    def get_table(self, table: Table):
        select_list = list(map(lambda c: c.select_str, [column for column in table.columns]))
        column_list = list(map(lambda c: c.name, [column for column in table.columns]))
        column_select = ','.join(select_list)
        with self.db.cursor() as cursor:
            cursor.rowfactory = lambda *args: dict(zip(column_list, args))
            for row in cursor.execute(f"SELECT {column_select} FROM {table.name}"):
                yield dict(zip(column_list, row))
