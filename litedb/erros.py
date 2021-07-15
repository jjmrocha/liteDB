class LiteDBError(Exception):
    pass


class BucketNotFound(LiteDBError):
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.message = f'Bucket {bucket_name} not found'
        super().__init__(self.message)


class InvalidKey(LiteDBError):
    def __init__(self, key_count: int):
        self.key_count = key_count
        self.message = 'Bucket must have only 1 key' if key_count > 1 else 'Bucket must have a key'
        super().__init__(self.message)


class BucketSchemaChanged(LiteDBError):
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.message = f'Bucket {bucket_name} was changed'
        super().__init__(self.message)


class InvalidSchemaChange(LiteDBError):
    def __init__(self, msg: str):
        self.message = msg
        super().__init__(self.message)


class RepositoryIsClosed(LiteDBError):
    def __init__(self, repository_name: str):
        self.repository_name = repository_name
        super().__init__(
            f'Repository {repository_name} is closed'
            if repository_name is not None
            else 'In memory repository is closed'
        )
