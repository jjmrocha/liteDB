import json
import sqlite3
from typing import Dict, List, Tuple, Callable, Any, Iterable

from litedb.erros import InvalidSchemaChange
from litedb.model import Field


class DB:
    def __init__(self, file_name: str):
        self.file_name = file_name
        self.conn = sqlite3.connect(file_name)
        with self.conn:
            self.conn.execute(
                """
                create table if not exists litedb_catalog (
                bucket_name text primary key,
                schema text not null)
                """
            )

    def catalog(self) -> Dict[str, List[Field]]:
        cur = self.conn.execute('select bucket_name, schema from litedb_catalog')
        return {
            bucket: decode_schema(schema)
            for bucket, schema in cur.fetchall()
        }

    def schema(self, name: str) -> List[Field]:
        cur = self.conn.execute('select schema from litedb_catalog where bucket_name=:name', {'name': name})
        row = cur.fetchone()
        return decode_schema(row[0])

    def create(self, name: str, schema: List[Field]):
        with self.conn:
            cur = self.conn.cursor()
            # Add entry to catalog
            catalog_entry = {
                'name': name,
                'schema': encode_schema(schema),
            }
            cur.execute('insert into litedb_catalog (bucket_name, schema) values (:name, :schema)', catalog_entry)
            # Create table
            cur.execute(sql_create_table(name, schema))
            # Create indexes
            for field in schema:
                if field.indexed:
                    cur.execute(sql_create_index(name, field.name))

    def alter(self, name: str, new_schema: List[Field]):
        old_schema = self.schema(name)
        # Check key not changed
        old_key = get_key(old_schema)
        new_key = get_key(new_schema)
        if old_key != new_key:
            raise InvalidSchemaChange("Schema key can't be changed")
        # Update catalog
        with self.conn:
            cur = self.conn.cursor()
            # Add entry to catalog
            catalog_entry = {
                'name': name,
                'schema': encode_schema(new_schema),
            }
            cur.execute('update litedb_catalog set schema = :schema where bucket_name = :name', catalog_entry)
            # Diff schema
            old_indices = filter(lambda x: x.indexed, old_schema)
            new_indices = filter(lambda x: x.indexed, new_schema)
            deleted_indices, added_indices = diff(old_indices, new_indices, lambda x: x.name)
            deleted_columns, added_columns = diff(old_schema, new_schema, lambda x: x.name)
            # Drop indices
            for column in deleted_indices:
                cur.execute(sql_drop_index(name, column))
            # Drop columns
            for column in deleted_columns:
                cur.execute(sql_drop_column(name, column))
            # Add columns
            for column in added_columns:
                cur.execute(sql_add_column(name, column))
            # Create new indices
            for column in added_indices:
                cur.execute(sql_create_index(name, column))

    def drop(self, name: str):
        with self.conn:
            cur = self.conn.cursor()
            cur.execute('delete from litedb_catalog where bucket_name=:name', {'name': name})
            cur.execute(f'drop table {name}')

    def close(self):
        self.conn.close()


def decode_schema(schema: str) -> List[Field]:
    return [
        Field.from_dict(field_dict)
        for field_dict in json.loads(schema)
    ]


def encode_schema(schema: List[Field]) -> str:
    dict_list = [
        field.to_dict()
        for field in schema
    ]
    return json.dumps(dict_list)


def sql_create_table(table: str, schema: List[Field]) -> str:
    columns = [
        f'{field.name} primary key' if field.is_key else field.name
        for field in schema
    ]
    return f'create table {table} ({",".join(columns)})'


def sql_add_column(table: str, column: str) -> str:
    return f'alter table {table} add column {column}'


def sql_drop_column(table: str, column: str) -> str:
    return f'alter table {table} drop column {column}'


def sql_create_index(table: str, column: str) -> str:
    return f'create index idx_{table}_{column} on {table} ({column})'


def sql_drop_index(table: str, column: str) -> str:
    return f'drop index if exists idx_{table}_{column}'


def get_key(schema: List[Field]) -> str:
    return list(filter(lambda x: x.is_key, schema))[0].name


def diff(old_schema: Iterable[Field],
         new_schema: Iterable[Field],
         mapper: Callable[[Field], Any]) -> Tuple[List[Any], List[Any]]:
    old_values = list(map(mapper, old_schema))
    new_values = list(map(mapper, new_schema))
    new_list = [
        x
        for x in new_values
        if x not in old_values
    ]
    old_list = [
        x
        for x in old_values
        if x not in new_values
    ]
    return old_list, new_list
