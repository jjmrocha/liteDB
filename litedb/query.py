from enum import Enum
from typing import Any, List


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
    ASC = 'asc'
    DESC = 'desc'


class Query:
    def __and__(self, other: 'Query') -> 'Query':
        return ComposedCondition(self, QueryOperator.AND, other)

    def __or__(self, other: 'Query') -> 'Query':
        return ComposedCondition(self, QueryOperator.OR, other)


class ComposedCondition(Query):
    def __init__(self, left: Query, operator: QueryOperator, right: Query):
        self.left = left
        self.operator = operator
        self.right = right

    def __str__(self):
        return f'({str(self.left)} {self.operator.value} {str(self.right)})'


class Condition(Query):
    def __init__(self, field_name: str):
        self.field_name = field_name
        self.operator = None
        self.target = None

    def __str__(self):
        return f'({self.field_name} {self.operator.value} {_str_target_(self.target)})'

    def equal_to(self, target: Any) -> Query:
        self.operator = QueryOperator.EQ
        self.target = target
        return self

    def not_equal_to(self, target: Any) -> Query:
        self.operator = QueryOperator.NE
        self.target = target
        return self

    def less_than(self, target: Any) -> Query:
        self.operator = QueryOperator.LT
        self.target = target
        return self

    def less_or_equal_to(self, target: Any) -> Query:
        self.operator = QueryOperator.LE
        self.target = target
        return self

    def greater_than(self, target: Any) -> Query:
        self.operator = QueryOperator.GT
        self.target = target
        return self

    def greater_or_equal_to(self, target: Any) -> Query:
        self.operator = QueryOperator.GE
        self.target = target
        return self

    def exists_in(self, target: List[Any]) -> Query:
        self.operator = QueryOperator.ANY
        self.target = target
        return self


def where(field_name: str) -> Condition:
    return Condition(field_name)


class Sort:
    def __and__(self, other: 'Sort') -> 'Sort':
        return SortOrder(self, other)


class OrderBy(Sort):
    def __init__(self, field_name: str, sort_type: QuerySort):
        self.field_name = field_name
        self.sort_type = sort_type

    def __str__(self):
        return f'{self.field_name} {self.sort_type.value}'


class SortOrder(Sort):
    def __init__(self, first: Sort, second: Sort):
        self.sort_order = first if isinstance(first, List) else [first]
        self.sort_order.append(second)

    def __str__(self):
        return ", ".join(map(lambda x: str(x), self.sort_order))


def asc(field_name: str) -> Sort:
    return OrderBy(field_name, QuerySort.ASC)


def desc(field_name: str) -> Sort:
    return OrderBy(field_name, QuerySort.DESC)


def _str_target_(value) -> str:
    if isinstance(value, List):
        str_values = map(_str_target_, value)
        return f'({",".join(str_values)})'
    if isinstance(value, str):
        return f'"{value}"'
    return str(value)
