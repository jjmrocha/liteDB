from os import path

import pytest

from litedb import (RepositoryIsClosed, Repository)


def test_close(tempdir):
    # given
    file_path = path.join(tempdir, 'test.ldb')
    repo = Repository(file_path)
    # when
    repo.close()
    # then
    assert repo.buckets == set()
    with pytest.raises(RepositoryIsClosed):
        repo.bucket('any')
    with pytest.raises(RepositoryIsClosed):
        repo.create_bucket('any', [])
    with pytest.raises(RepositoryIsClosed):
        repo.drop_bucket('any')
    with pytest.raises(RepositoryIsClosed):
        repo.close()
