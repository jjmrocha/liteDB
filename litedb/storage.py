from typing import Optional, Dict, Any, List, Tuple, Iterable

from litedb.catalog import DB
from litedb.model import Field
from litedb.query import Sort, Query


class Table:
    def __init__(self, name: str, schema: List[Field]):
        self.name = name
        self.schema = schema
        self.key = find_bucket_key(schema)
        self.fields = [
            field.name
            for field in schema
        ]
        self.template = {
            field: None
            for field in self.fields
        }
        self._sql_delete_ = sql_delete(self.name, self.key)
        self._sql_insert_ = sql_insert(self.name, self.fields)
        self._sql_upsert_ = sql_upsert(self.name, self.key, self.fields)
        self._sql_find_by_pk_ = sql_find_by_pk(self.name, self.fields, self.key)
        self._sql_find_all_ = sql_find_all(self.name, self.fields)
        self._sql_count_ = sql_count(self.name)

    def insert(self, db: DB, items: Iterable[Dict[str, Any]]):
        self._store_(db, self._sql_insert_, items)

    def upsert(self, db: DB, items: Iterable[Dict[str, Any]]):
        self._store_(db, self._sql_upsert_, items)

    def _store_(self, db: DB, sql: str, items: Iterable[Dict[str, Any]]):
        full_item = map(lambda item: self.template | item, items)
        with db.conn:
            cur = db.conn.cursor()
            cur.executemany(sql, full_item)

    def delete(self, db: DB, key: Any):
        with db.conn:
            cur = db.conn.cursor()
            cur.execute(self._sql_delete_, {'key': key})

    def find_by_key(self, db: DB, key: Any) -> Optional[Dict[str, Any]]:
        cur = db.conn.cursor()
        cur.execute(self._sql_find_by_pk_, {'key': key})
        values = cur.fetchone()
        return to_item(self.fields, values) if values is not None else None

    def fetch_all(self, db: DB) -> Iterable[Dict[str, Any]]:
        return self._iterable_(db, self._sql_find_all_)

    def fetch(self, db: DB, query: Query, sort: Optional[Sort]) -> Iterable[Dict[str, Any]]:
        sql = sql_filter(self.name, self.fields, query, sort)
        return self._iterable_(db, sql)

    def _iterable_(self, db: DB, sql: str) -> Iterable[Dict[str, Any]]:
        cur = db.conn.cursor()
        cur.execute(sql)
        values = cur.fetchone()
        while values is not None:
            yield to_item(self.fields, values)
            values = cur.fetchone()

    def count(self, db: DB) -> int:
        cur = db.conn.cursor()
        cur.execute(self._sql_count_)
        values = cur.fetchone()
        return values[0]


def find_bucket_key(schema: List[Field]) -> str:
    for field in schema:
        if field.is_key:
            return field.name


def sql_delete(table: str, key: str) -> str:
    return f'delete from {table} where {key}=:key'


def sql_insert(table: str, fields: List[str]) -> str:
    fields_str = ','.join(fields)
    params_str = ','.join(map(lambda x: f':{x}', fields))
    return f'insert into {table}({fields_str}) values ({params_str})'


def sql_upsert(table: str, key: str, fields: List[str]) -> str:
    insert_str = sql_insert(table, fields)
    update_str = ','.join(map(lambda x: f'{x}=:{x}', fields))
    return f'{insert_str} on conflict({key}) do update set {update_str} where {key}=:{key}'


def sql_find_by_pk(table: str, fields: List[str], key: str) -> str:
    fields_str = ','.join(fields)
    return f'select {fields_str} from {table} where {key}=:key'


def sql_find_all(table: str, fields: List[str]) -> str:
    fields_str = ','.join(fields)
    return f'select {fields_str} from {table}'


def sql_count(table: str) -> str:
    return f'select count(*) from {table}'


def to_item(fields: List[str], values: Tuple) -> Dict[str, Any]:
    return {
        key: value
        for key, value in zip(fields, values)
    }


def sql_filter(table: str, fields: List[str], query: Query, sort: Optional[Sort]) -> str:
    fields_str = ','.join(fields)
    where_clause = str(query)
    if sort is None:
        return f'select {fields_str} from {table} where {where_clause}'
    else:
        sort_clause = str(sort)
        return f'select {fields_str} from {table} where {where_clause} order by {sort_clause}'
