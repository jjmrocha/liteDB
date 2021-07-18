from typing import Any, List, Dict, Iterable, Optional

from litedb.model import Field
from litedb.query import Sort, Query
from litedb.storage import Table, DB


class Bucket:
    def __init__(self, db: DB, name: str, schema: List[Field]):
        self._db_ = db
        self._table_ = Table(name, schema)

    def __str__(self):
        return f'{self.__class__.__name__}({self.name}, {self.schema})'

    def __repr__(self):
        return f'<bucket name={self.name}, schema={self.schema}>'

    @property
    def name(self) -> str:
        return self._table_.name

    @property
    def schema(self) -> List[Field]:
        return self._table_.schema

    def save(self, item: Dict[str, Any], update_if_exists: bool = True):
        if update_if_exists:
            self._table_.upsert(self._db_, [item])
        else:
            self._table_.insert(self._db_, [item])

    def save_all(self, items: Iterable[Dict[str, Any]], update_if_exists: bool = True):
        if update_if_exists:
            self._table_.upsert(self._db_, items)
        else:
            self._table_.insert(self._db_, items)

    def delete(self, key: Any):
        self._table_.delete(self._db_, key)

    def __getitem__(self, key: Any) -> Optional[Dict[str, Any]]:
        return self.get(key)

    def get(self, key: Any) -> Optional[Dict[str, Any]]:
        return self._table_.find_by_key(self._db_, key)

    def __iter__(self) -> Iterable[Dict[str, Any]]:
        return self.all()

    def all(self) -> Iterable[Dict[str, Any]]:
        return self._table_.fetch_all(self._db_)

    def filter(self, query: Query, sort: Optional[Sort] = None) -> Iterable[Dict[str, Any]]:
        return self._table_.fetch(self._db_, query, sort)

    def __len__(self):
        return self.count()

    def count(self) -> int:
        return self._table_.count(self._db_)
