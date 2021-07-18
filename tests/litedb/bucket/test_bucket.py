from litedb import Field


def test_new(stateless_repo):
    # given
    bucket = stateless_repo.create_bucket(
        name='test_bucket',
        schema=[
            Field('id', is_key=True),
            Field('name'),
            Field('age'),
        ]
    )
    # then
    assert bucket.name == 'test_bucket'
    assert bucket.schema == [
            Field('id', is_key=True),
            Field('name'),
            Field('age'),
        ]


