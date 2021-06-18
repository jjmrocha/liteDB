from typing import Optional, Dict

from litedb.bucket import Bucket
from litedb.schema import DB
from litedb.schema import Field


class Repository:
    def __init__(self, file_name: str):
        self._db_ = DB(file_name)
        self.schemas = {
            name: schema
            for name, schema in self._db_.buckets()
        }

    def __str__(self):
        return f'{self.__class__.__name__}({self.file_name})'

    @property
    def file_name(self) -> str:
        return self._db_.file_name

    def bucket(self, name: str) -> Optional[Bucket]:
        schema = self.schemas.get(name)
        if schema is None:
            return None
        return Bucket(
            db=self._db_,
            name=name,
            schema=schema,
        )

    def create_or_update_bucket(self, name: str, schema: Dict[str, Field]) -> Bucket:
        old_schema = self.schemas.get(name)

        if old_schema is None:
            self._db_.create(name, schema)
            self.schemas[name] = schema
        elif old_schema != schema:
            self._db_.alter(name, schema)
            self.schemas[name] = schema

        return self.bucket(name)
