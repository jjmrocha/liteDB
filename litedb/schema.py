from typing import Iterable, Tuple, Optional, Dict, Any

from litedb.models import Field
from litedb.query import Query, Sort


class DB:
    def __init__(self, file_name: str):
        self.file_name = file_name

    def buckets(self) -> Iterable[Tuple[str, Dict[str, Field]]]:
        pass

    def create(self, name: str, schema: Dict[str, Field]):
        pass

    def drop(self, name):
        pass

    def alter(self, name: str, new_schema: Dict[str, Field]):
        pass


class Store:
    def __init__(self, name: str, schema: Dict[str, Field]):
        self.name = name
        self.schema = schema

    def insert(self, db: DB, items: Iterable[Dict[str, Any]]):
        pass

    def upsert(self, db: DB, items: Iterable[Dict[str, Any]]):
        pass

    def delete(self, db: DB, key: Any):
        pass

    def find_by_key(self, db: DB, key: Any) -> Optional[Dict[str, Any]]:
        pass

    def fetch(self, query: Query, sort: Optional[Sort]) -> Iterable[Dict[str, Any]]:
        pass
