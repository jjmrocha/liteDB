from typing import Any, Dict
from typing import Iterable, Optional

from litedb.query import Query, Sort
from litedb.schema import Store, DB, Field


class Bucket:
    def __init__(self, db: DB, name: str, schema: Dict[str, Field]):
        self._db_ = db
        self._store_ = Store(name, schema)

    def __str__(self):
        return f'{self.__class__.__name__}({self.name}, {self.schema})'

    @property
    def name(self) -> str:
        return self._store_.name

    @property
    def schema(self) -> Dict[str, Field]:
        return self._store_.schema

    def store(self, item: Dict[str, Any], update_if_exists: bool = True):
        if update_if_exists:
            self._store_.upsert(self._db_, [item])
        else:
            self._store_.insert(self._db_, [item])

    def batch(self, items: Iterable[Dict[str, Any]], update_if_exists: bool = True):
        if update_if_exists:
            self._store_.upsert(self._db_, items)
        else:
            self._store_.insert(self._db_, items)

    def remove(self, key: Any):
        self._store_.delete(self._db_, key)

    def get(self, key: Any) -> Optional[Dict[str, Any]]:
        return self._store_.find_by_key(self._db_, key)

    def filter(self, query: Query, sort: Optional[Sort] = None) -> Iterable[Dict[str, Any]]:
        return self._store_.fetch(query, sort)
