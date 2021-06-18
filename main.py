from litedb import fields
from litedb.database import Repository
from litedb.query import where, asc, desc

if __name__ == '__main__':
    repo = Repository('sample.db')
    bucket = repo.create_or_update_bucket(
        name='developers',
        schema={
            'id': fields.Integer(is_key=True),
            'name': fields.String(indexed=True),
            'tag': fields.String(),
        }
    )
    bucket.store({
        'id': 1,
        'name': 'Joaquim',
        'tag': 'java',
    })
    results = bucket.filter(
        query=where('name').equal_to('Joaquim') & where('tag').exists_in(['java', 'scala']),
        sort=desc('tag') & asc('name')
    )

    for x in results:
        print(x)
