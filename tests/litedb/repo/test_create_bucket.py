import pytest

from litedb import (InvalidKey, BucketSchemaChanged, Field)


def test_invalid_schema(stateless_repo):
    # given
    assert stateless_repo.buckets == set()
    # when
    with pytest.raises(InvalidKey):
        stateless_repo.create_bucket(
            name='bucket',
            schema=[
                Field('id')
            ],
            update_if_needed=False
        )


def test_no_bucket(stateless_repo):
    # given
    assert stateless_repo.buckets == set()
    # when
    bucket = stateless_repo.create_bucket(
        name='bucket',
        schema=[
            Field('id', is_key=True),
            Field('name')
        ],
        update_if_needed=False
    )
    # then
    assert bucket is not None
    assert stateless_repo.buckets == {'bucket'}


def test_bucket_exists_same_schema(stateless_repo):
    # given
    stateless_repo.create_bucket(
        name='bucket',
        schema=[
            Field('id', is_key=True),
            Field('name')
        ],
        update_if_needed=False
    )
    # when
    bucket = stateless_repo.create_bucket(
        name='bucket',
        schema=[
            Field('id', is_key=True),
            Field('name')
        ],
        update_if_needed=False
    )
    # then
    assert bucket is not None
    assert stateless_repo.buckets == {'bucket'}


def test_bucket_exists_different_schema_update_allowed(stateless_repo):
    # given
    stateless_repo.create_bucket(
        name='bucket',
        schema=[
            Field('id', is_key=True),
            Field('name')
        ],
        update_if_needed=False
    )
    # when
    bucket = stateless_repo.create_bucket(
        name='bucket',
        schema=[
            Field('id', is_key=True),
            Field('name', indexed=True)
        ],
        update_if_needed=True
    )
    # then
    assert bucket is not None
    assert stateless_repo.buckets == {'bucket'}


def test_bucket_exists_different_schema_update_not_allowed(stateless_repo):
    # given
    stateless_repo.create_bucket(
        name='bucket',
        schema=[
            Field('id', is_key=True),
            Field('name')
        ],
        update_if_needed=False
    )
    # when
    with pytest.raises(BucketSchemaChanged):
        stateless_repo.create_bucket(
            name='bucket',
            schema=[
                Field('id', is_key=True),
                Field('name', indexed=True)
            ],
            update_if_needed=False
        )
