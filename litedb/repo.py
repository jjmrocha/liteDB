from typing import List, Set
from typing import Optional

from litedb.bucket import Bucket
from litedb.catalog import DB
from litedb.erros import BucketNotFound, InvalidKey, BucketSchemaChanged
from litedb.model import Field


class Repository:
    def __init__(self, file_name: str = ':memory:'):
        self._db_ = DB(file_name)
        self.schemas = self._db_.catalog()

    def __str__(self):
        return f'{self.__class__.__name__}({self.file_name})'

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @property
    def file_name(self) -> str:
        return self._db_.file_name

    @property
    def buckets(self) -> Set[str]:
        return set(self.schemas.keys())

    def bucket(self, name: str) -> Optional[Bucket]:
        schema = self.schemas.get(name)
        if schema is None:
            raise BucketNotFound(name)
        return Bucket(
            db=self._db_,
            name=name,
            schema=schema,
        )

    def schema(self, name: str) -> List[Field]:
        return self._db_.schema(name)

    def create_bucket(self, name: str, schema: List[Field], update_if_needed: bool = False) -> Bucket:
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
        self.schemas.pop('name', None)
        self._db_.drop(name)

    def close(self):
        self._db_.close()


def check_key(schema):
    key_count = len(list(filter(lambda x: x.is_key, schema)))
    if key_count != 1:
        raise InvalidKey(key_count)
