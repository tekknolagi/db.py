# Python SQL-like Database

This project is a Python-based, in-memory database that provides a simplified, SQL-like interface for data manipulation. It is a medium-faithful port of the original [SQLToy](https://github.com/weinberg/SQLToy) project.

## Core Concepts

The two primary components of this library are the `Database` and `Table` classes.

- **`Database`**: Manages a collection of tables and provides methods for performing SQL-like operations.
- **`Table`**: Represents a collection of rows, where each row is a dictionary of column-value pairs.

## Basic Operations

### Creating a Table

You can create a new table within a database using the `CREATE_TABLE` method.

```python
from db import Database

db = Database()
db.CREATE_TABLE("users", colnames=["id", "name"])
```

### Inserting Data

To add data to a table, use the `INSERT_INTO` method.

```python
db.INSERT_INTO("users", [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"}
])
```

### Selecting Data

The `SELECT` method allows you to retrieve specific columns from a table.

```python
users_table = db.tables["users"]
names = db.SELECT(users_table, ["name"])
# names.rows will be ({'name': 'Alice'}, {'name': 'Bob'})
```

## Advanced Queries

This library supports a variety of more complex query operations.

### Joining Tables

You can perform `INNER JOIN`, `LEFT JOIN`, and `RIGHT JOIN` operations.

```python
# Create another table
db.CREATE_TABLE("posts", colnames=["user_id", "title"])
db.INSERT_INTO("posts", [
    {"user_id": 1, "title": "First Post"},
    {"user_id": 2, "title": "Second Post"}
])

# Perform an inner join
user_posts = db.JOIN(
    db.tables["users"],
    db.tables["posts"],
    lambda row: row["users.id"] == row["posts.user_id"]
)
```

### Grouping and Aggregation

The library supports `GROUP_BY` and aggregate functions like `COUNT`, `MAX`, and `SUM`.

```python
from db import Table

friends = Table(
    "friends",
    [
        {"id": 1, "city": "Denver", "state": "Colorado"},
        {"id": 2, "city": "Houston", "state": "Texas"},
        {"id": 3, "city": "Colorado Springs", "state": "Colorado"},
    ],
)

# Group by state
grouped_by_state = db.GROUP_BY(friends, ["state"])

# Count cities in each state
state_counts = db.COUNT(grouped_by_state, "city")
# state_counts.rows will be ({'state': 'Colorado', 'COUNT(city)': 2}, {'state': 'Texas', 'COUNT(city)': 1})
```

### The `query` Function

For more complex, multi-step queries, you can use the `query` function, which chains together multiple operations in a declarative way.

```python
from db import query

# A complex query example
result = query(
    db,
    select=["employee.name", "department.title"],
    from_=["employee"],
    join=[
        [
            "department",
            lambda row: row["employee.department_id"] == row["department.id"],
        ],
    ],
    where=[lambda row: row["employee.salary"] > 150],
)
```

## Running Tests

To ensure everything is working correctly, you can run the included test suite from your terminal:

```bash
python -m unittest db_tests.py
