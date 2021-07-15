import pytest

from litedb import (InvalidKey, Field)
from litedb.repo import check_key


def test_no_key():
    # given
    schema = [
        Field('field1'),
        Field('field2')
    ]
    # when
    with pytest.raises(InvalidKey):
        check_key(schema)


def test_too_many_keys():
    # given
    schema = [
        Field('field1', is_key=True),
        Field('field2', is_key=True)
    ]
    # when
    with pytest.raises(InvalidKey):
        check_key(schema)


def test_success():
    # given
    schema = [
        Field('field1', is_key=True),
        Field('field2')
    ]
    # when
    check_key(schema)
