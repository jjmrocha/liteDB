import tempfile
from os import path

import pytest

from litedb import Repository, Field


@pytest.fixture
def tempdir():
    with tempfile.TemporaryDirectory() as temp:
        yield temp


@pytest.fixture
def stateful_repo(tempdir):
    file_path = path.join(tempdir, 'sfr.ldb')
    with Repository(file_path) as repo:
        yield repo


@pytest.fixture
def stateless_repo():
    with Repository() as repo:
        yield repo


@pytest.fixture
def bucket(stateless_repo):
    yield stateless_repo.create_bucket(
        name='test_bucket',
        schema=[
            Field('id', is_key=True),
            Field('name'),
            Field('age', indexed=True),
        ]
    )
