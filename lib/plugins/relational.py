##
##

import attr
from attr.validators import instance_of as io


@attr.s
class Column(object):
    name = attr.ib(validator=io(str))
    data_type = attr.ib(validator=io(str))
    select_str = attr.ib(validator=io(str))

    @classmethod
    def add(cls, name: str, data_type: str, select_str: str):
        return cls(
            name,
            data_type,
            select_str
        )

    @property
    def as_dict(self):
        return self.__dict__


@attr.s
class Table(object):
    name = attr.ib(validator=io(str))
    size = attr.ib(validator=io(int))
    rows = attr.ib(validator=io(int))
    columns = attr.ib(type=list[Column])

    @classmethod
    def build(cls, name: str, size: int, rows: int):
        return cls(
            name,
            size,
            rows,
            []
        )

    def add(self, column: Column):
        self.columns.append(column)
        return self

    @property
    def as_dict(self):
        return self.__dict__


@attr.s
class Schema(object):
    tables = attr.ib(type=list[Table])

    @classmethod
    def build(cls):
        return cls(
            []
        )

    def add(self, table: Table):
        self.tables.append(table)
        return self

    @property
    def as_dict(self):
        return self.__dict__
