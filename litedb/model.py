from typing import Dict


class Field:
    def __init__(self, name: str, is_key: bool = False, indexed: bool = False):
        self.name = name
        self.is_key = is_key
        self.indexed = indexed

    def __str__(self):
        return f'{self.__class__.__name__}({self.name}, {self.is_key}, {self.indexed})'

    def __eq__(self, other) -> bool:
        if not isinstance(other, Field):
            return False
        if self.name != other.name:
            return False
        if self.is_key != other.is_key:
            return False
        if self.indexed != other.indexed:
            return False
        return True

    def to_dict(self):
        return {
            'name': self.name,
            'is_key': self.is_key,
            'indexed': self.indexed,
        }

    @classmethod
    def from_dict(cls, props: Dict):
        return cls(
            name=props['name'],
            is_key=props.get('is_key', False),
            indexed=props.get('indexed', False),
        )
