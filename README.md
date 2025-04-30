# liteDB

LiteDB is a lightweight wrapper for SQLite3, designed to simplify database interactions by providing an easy-to-use interface for managing data with buckets and schemas.

## Features

- Lightweight and easy-to-use SQLite3 wrapper.
- Support for in-memory and file-based repositories.
- Schema-based bucket creation and management.
- Query data with filtering and sorting.
- Automatic schema validation and indexing.

## Filter DSL (Domain-Specific Language)

LiteDB provides a powerful and expressive DSL for querying data. The DSL allows you to build complex queries using conditions and logical operators.

### Basic Conditions

You can create conditions using the `where` function and apply comparison operators:

- `equal_to(value)`: Matches records where the field is equal to the given value.
- `not_equal_to(value)`: Matches records where the field is not equal to the given value.
- `less_than(value)`: Matches records where the field is less than the given value.
- `less_or_equal_to(value)`: Matches records where the field is less than or equal to the given value.
- `greater_than(value)`: Matches records where the field is greater than the given value.
- `greater_or_equal_to(value)`: Matches records where the field is greater than or equal to the given value.
- `exists_in(values)`: Matches records where the field exists in the given list of values.

Example:

```python
from litedb.query import where

# Filter records where age is greater than 25
condition = where("age").greater_than(25)
```

### Logical Operators
You can combine multiple conditions using logical operators:

- `&`: Combines two conditions with a logical AND.
- `|`: Combines two conditions with a logical OR.

Example:
```python
from litedb.query import where, and_

# Filter records where age is greater or equal to 18 AND less than 65
condition = where("age").greater_or_equal_to(18) & where("age").less_than(65)
```

### Sorting

You can sort query results using the asc and desc functions:

- `asc(field_name)`: Sorts results in ascending order by the specified field.
- `desc(field_name)`: Sorts results in descending order by the specified field.

And combine multiple sort opeartions with `&`.

Example:
```python
from litedb.query import asc, desc

# Sort results by age in ascending order, then by name in descending order
sort_order = asc("age") & desc("name")
```

## Usage
### Creating a Repository
You can create an in-memory repository or a file-based repository:

```python
from litedb import Repository

# In-memory repository
with Repository() as repo:
    print("In-memory repository created")

# File-based repository
with Repository("data.ldb") as repo:
    print("File-based repository created")
```

### Creating a Bucket
Buckets are created with a schema that defines the fields and their properties:

```python
from litedb import Field

with Repository() as repo:
    bucket = repo.create_bucket(
        name="users",
        schema=[
            Field("id", is_key=True),
            Field("name"),
            Field("age", indexed=True),
        ]
    )
    print(f"Bucket created: {bucket.name}")
```

### Opening a Bucket
If a bucket already exists in the repository, you can open it by its name:

```python
with Repository("data.ldb") as repo:
    bucket = repo.bucket("users")
    print(f"Opened bucket: {bucket.name}")

    # Example: Fetch all data from the bucket
    for item in bucket:
        print(item)
```

### Adding Data to a Bucket
You can add data to a bucket using the save or save_all methods:

```python
bucket.save({"id": 1, "name": "Alice", "age": 30}, update_if_exists=False)
bucket.save_all([
    {"id": 2, "name": "Bob", "age": 25},
    {"id": 3, "name": "Charlie", "age": 35},
])
```

### Get the Bucket size
You can get the number of the items on the bucket using two methods:

```python
number_of_items = bucket.count():
print(number_of_items)

# Or just using the len function
bucket_size = len(bucket)
print(bucket_size)
```

### Fetch all data from a Bucket
You can get access all data store in a bucket by using an ierator:

```python
for item in bucket.all():
    print(item)

# Or just using the bucket object
for item in bucket:
    print(item)
```

### Querying Data
You can query data using filters and sorting:

```python
from litedb.query import where, asc

# Filter data
results = bucket.filter(where("age").greater_than(25))
for item in results:
    print(item)

# Sort data
sorted_results = bucket.filter(query=where("age").greater_than(25), sort=asc("age"))
for item in sorted_results:
    print(item)
```

### Accessing Data by Key
You can retrieve data by its key:

```python
user = bucket[1] # Or bucket.get(1)
print(user)  # Output: {'id': 1, 'name': 'Alice', 'age': 30}
```

### Deleting Data
You can delete data by its key:

```python
bucket.delete(1)
```

### Dropping a Bucket
To remove a bucket from the repository:

```python
repo.drop_bucket("users")
```

### Closing a Repository
Always close the repository when you're done, or use the `with` statement to ensure its always closed:

```python
repo.close()
```

## Running Tests
To run the tests, use `pytest`:

```sh
pytest
```

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.