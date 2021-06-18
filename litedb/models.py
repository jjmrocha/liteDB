from enum import Enum


class Type(Enum):
    STRING = 1
    BOOLEAN = 2
    INTEGER = 3
    FLOAT = 4


class QueryOperator(Enum):
    AND = 'and'
    OR = 'or'
    EQ = '=='
    NE = '!='
    LT = '<'
    LE = '<='
    GT = '>'
    GE = '>='
    ANY = 'in'


class QuerySort(Enum):
    ASC = 1
    DESC = 2


class Field:
    data_type = None

    def __init__(self, is_key: bool = False, indexed: bool = False):
        self.is_key = is_key
        self.indexed = indexed

    def __str__(self):
        return f'{self.__class__.__name__}({self.data_type.name}, {self.is_key}, {self.indexed})'
