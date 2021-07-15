from typing import List, Set

from litedb.bucket import Bucket
from litedb.catalog import DB
from litedb.erros import (BucketNotFound, InvalidKey, BucketSchemaChanged, RepositoryIsClosed)
from litedb.model import Field


class Repository:
    def __init__(self, repository_name: str = None):
        self.is_closed = False
        self.in_memory = repository_name is None
        self.repository_name = repository_name
        self._db_ = DB(':memory:' if self.in_memory else repository_name)
        self.schemas = self._db_.catalog()

    def __str__(self):
        return f'{self.__class__.__name__}({self.repository_name})'

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @property
    def buckets(self) -> Set[str]:
        return set(self.schemas.keys())

    def bucket(self, name: str) -> Bucket:
        self._check_repository_is_open_()
        schema = self.schemas.get(name)
        if schema is None:
            raise BucketNotFound(name)
        return Bucket(
            db=self._db_,
            name=name,
            schema=schema,
        )

    def create_bucket(self, name: str, schema: List[Field], update_if_needed: bool = False) -> Bucket:
        self._check_repository_is_open_()
        # Check number of keys
        check_key(schema)
        # Check if bucket exists
        old_schema = self.schemas.get(name)

        if old_schema is None:
            self._db_.create(name, schema)
            self.schemas[name] = schema
        elif old_schema != schema:
            if update_if_needed:
                self._db_.alter(name, schema)
                self.schemas[name] = schema
            else:
                raise BucketSchemaChanged(name)

        return self.bucket(name)

    def drop_bucket(self, name: str):
        self._check_repository_is_open_()
        schema = self.schemas.pop(name, None)
        if schema is not None:
            self._db_.drop(name)

    def close(self):
        self._check_repository_is_open_()
        self._db_.close()
        self.schemas = {}
        self.is_closed = True

    def _check_repository_is_open_(self):
        if self.is_closed:
            raise RepositoryIsClosed(self.repository_name)


def check_key(schema: List[Field]):
    key_count = len(list(filter(lambda x: x.is_key, schema)))
    if key_count != 1:
        raise InvalidKey(key_count)
