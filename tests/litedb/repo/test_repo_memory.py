import pytest

from litedb import (BucketNotFound, Repository, Field)


def test_new():
    # when
    with Repository() as class_under_test:
        # then
        assert class_under_test.repository_name is None
        assert class_under_test.in_memory
        assert class_under_test.buckets == set()


def test_create_new_bucket(stateless_repo):
    # given
    assert stateless_repo.in_memory
    assert stateless_repo.buckets == set()
    with pytest.raises(BucketNotFound):
        stateless_repo.bucket('new_bucket')
    # when
    bucket = stateless_repo.create_bucket(
        name='new_bucket',
        schema=[
            Field('id', is_key=True),
            Field('name')
        ]
    )
    # then
    assert bucket is not None
    assert stateless_repo.buckets == {'new_bucket'}
    assert stateless_repo.bucket('new_bucket') is not None


def test_drop_bucket(stateless_repo):
    # given
    bucket = stateless_repo.create_bucket(
        name='test',
        schema=[
            Field('id', is_key=True),
            Field('name')
        ]
    )
    assert bucket is not None
    assert stateless_repo.buckets == {'test'}
    assert stateless_repo.bucket('test') is not None
    # when
    stateless_repo.drop_bucket('test')
    # then
    assert stateless_repo.buckets == set()
    with pytest.raises(BucketNotFound):
        stateless_repo.bucket('test')
