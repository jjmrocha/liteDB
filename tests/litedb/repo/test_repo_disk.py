from os import path

import pytest

from litedb import (BucketNotFound, Repository, Field)


def test_new(tempdir):
    # given
    file_path = path.join(tempdir, 'test.ldb')
    # when
    with Repository(file_path) as class_under_test:
        # then
        assert class_under_test.repository_name == file_path
        assert not class_under_test.in_memory
        assert class_under_test.buckets == set()


def test_create_new_bucket(stateful_repo):
    # given
    assert not stateful_repo.in_memory
    assert stateful_repo.buckets == set()
    with pytest.raises(BucketNotFound):
        stateful_repo.bucket('new_bucket')
    # when
    bucket = stateful_repo.create_bucket(
        name='new_bucket',
        schema=[
            Field('id', is_key=True),
            Field('name')
        ]
    )
    # then
    assert bucket is not None
    assert stateful_repo.buckets == {'new_bucket'}
    assert stateful_repo.bucket('new_bucket') is not None


def test_drop_bucket(stateful_repo):
    # given
    bucket = stateful_repo.create_bucket(
        name='test',
        schema=[
            Field('id', is_key=True),
            Field('name')
        ]
    )
    assert bucket is not None
    assert stateful_repo.buckets == {'test'}
    assert stateful_repo.bucket('test') is not None
    # when
    stateful_repo.drop_bucket('test')
    # then
    assert stateful_repo.buckets == set()
    with pytest.raises(BucketNotFound):
        stateful_repo.bucket('test')


def test_persistence(tempdir):
    # given
    file_path = path.join(tempdir, 'test.ldb')
    repo = Repository(file_path)
    bucket = repo.create_bucket(
        name='test',
        schema=[
            Field('id', is_key=True),
            Field('name')
        ]
    )
    assert bucket is not None
    assert repo.buckets == {'test'}
    assert repo.bucket('test') is not None
    # when
    repo.close()
    assert repo.is_closed
    new_repo = Repository(file_path)
    # then
    assert new_repo.buckets == {'test'}
    assert new_repo.bucket('test') is not None
