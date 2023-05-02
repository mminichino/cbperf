##
##

import logging
import uuid
from enum import Enum
from typing import Union
from lib.exceptions import KeyFormatError


class KeyStyle(Enum):
    DEFAULT = 0
    TYPE = 1
    UUID = 2
    FIELD = 3
    COLLECTION = 4
    COMPOUND = 5


class KeyFormat(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def key_format(style: KeyStyle,
                   document: dict,
                   keyspace: str,
                   doc_num: int,
                   id_key: str = "record_id",
                   separator: str = "::",
                   field: Union[str, None] = None):
        if style.value == 0:
            return f"{keyspace}:{doc_num}"
        elif style.value == 1:
            if not document.get('type'):
                raise KeyFormatError(f"Key style type requested: document does not have type field")
            if document.get(id_key):
                number = document.get(id_key)
            else:
                number = doc_num
            return f"{document['type']}{separator}{number}"
        elif style.value == 2:
            return uuid.uuid4()
        elif style.value == 3:
            if not field:
                raise KeyFormatError(f"Key field name style requested: field parameter is null")
            return f"{field}{separator}{doc_num}"
        elif style.value == 4:
            return f"{keyspace}{separator}{doc_num}"
        else:
            if not field:
                raise KeyFormatError(f"Key compound name style requested: field parameter is null")
            return f"{keyspace}{separator}{field}{separator}{doc_num}"
