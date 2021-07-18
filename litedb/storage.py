from typing import Optional, Dict, Any, List, Tuple, Iterable, Callable

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
        self.sql = SQL(self)

    def insert(self, db: DB, items: Iterable[Dict[str, Any]]):
        self._store_(db, self.sql.insert, items)

    def upsert(self, db: DB, items: Iterable[Dict[str, Any]]):
        self._store_(db, self.sql.upsert, items)

    def _store_(self, db: DB, sql: str, items: Iterable[Dict[str, Any]]):
        full_item = map(lambda item: self.template | item, items)
        with db.conn:
            cur = db.conn.cursor()
            cur.executemany(sql, full_item)

    def delete(self, db: DB, key: Any):
        with db.conn:
            cur = db.conn.cursor()
            cur.execute(self.sql.delete, {'key': key})

    def find_by_key(self, db: DB, key: Any) -> Optional[Dict[str, Any]]:
        cur = db.conn.cursor()
        cur.execute(self.sql.find_by_pk, {'key': key})
        values = cur.fetchone()
        return to_item(self.fields, values) if values is not None else None

    def fetch_all(self, db: DB) -> Iterable[Dict[str, Any]]:
        return self._iterable_(db, self.sql.find_all)

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
        cur.execute(self.sql.count)
        values = cur.fetchone()
        return values[0]


def find_bucket_key(schema: List[Field]) -> Optional[str]:
    for field in schema:
        if field.is_key:
            return field.name
    return None


def to_item(fields: List[str], values: Tuple) -> Dict[str, Any]:
    return dict(zip(fields, values))


class SQL:
    def __init__(self, table: Table):
        self.table = table
        self._delete_ = None
        self._insert_ = None
        self._upsert_ = None
        self._find_by_pk_ = None
        self._find_all_ = None
        self._count_ = None

    @property
    def delete(self) -> str:
        if self._delete_ is None:
            self._delete_ = sql_delete(self.table.name, self.table.key)
        return self._delete_

    @property
    def insert(self) -> str:
        if self._insert_ is None:
            _insert_ = sql_insert(self.table.name, self.table.fields)
        return self._insert_

    @property
    def upsert(self) -> str:
        if self._upsert_ is None:
            _upsert_ = sql_upsert(self.table.name, self.table.key, self.table.fields)
        return self._upsert_

    @property
    def find_by_pk(self) -> str:
        if self._find_by_pk_ is None:
            _find_by_pk_ = sql_find_by_pk(self.table.name, self.table.fields, self.table.key)
        return self._find_by_pk_

    @property
    def find_all(self) -> str:
        if self._find_all_ is None:
            _find_all_ = sql_find_all(self.table.name, self.table.fields)
        return self._find_all_

    @property
    def count(self) -> str:
        if self._count_ is None:
            _count_ = sql_count(self.table.name)
        return self._count_


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


def sql_filter(table: str, fields: List[str], query: Query, sort: Optional[Sort]) -> str:
    fields_str = ','.join(fields)
    where_clause = str(query)
    if sort is None:
        return f'select {fields_str} from {table} where {where_clause}'
    sort_clause = str(sort)
    return f'select {fields_str} from {table} where {where_clause} order by {sort_clause}'
