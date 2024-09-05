import functools


class Table:
    def __init__(self, name: str, rows: tuple[dict] = ()):
        self.name = name
        self.rows = tuple(rows)

    def filter(self, pred):
        return Table(self.name, [row for row in self.rows if pred(row)])

    def __repr__(self):
        if not self.name:
            return f"Table({list(self.rows)!r})"
        return f"Table({self.name!r}, {list(self.rows)!r})"


def empty_table(name=""):
    return Table(name)


US = "\x1f"  # Unit Separator


class Database:
    def __init__(self):
        self.tables = {}

    def CREATE_TABLE(self, name):
        table = empty_table(name)
        self.tables[name] = table
        return table

    def DROP_TABLE(self, name):
        del self.tables[name]

    def FROM(self, *table_names):
        match table_names:
            case (table_name,):
                return self.tables[table_name]
            case (table_name, *rest):
                return self.CROSS_JOIN(self.tables[table_name], self.FROM(*rest))

    def SELECT(self, table, columns, aliases=None):
        if aliases is None:
            aliases = {}
        return Table(
            table.name,
            [
                {aliases.get(col, col): row[col] for col in columns}
                for row in table.rows
            ],
        )

    def WHERE(self, table, pred):
        return Table(table.name, [row for row in table.rows if pred(row)])

    def INSERT_INTO(self, table_name, *rows):
        table = self.tables[table_name]
        table.rows = (*table.rows, *rows)

    def UPDATE(self, table, set, pred):
        return Table(
            table.name, [{**row, **set} if pred(row) else row for row in table.rows]
        )

    def CROSS_JOIN(self, a, b):
        rows = []
        for x in a.rows:
            for y in b.rows:
                row = {}
                for k in x:
                    column_name = f"{a.name}.{k}" if a.name else k
                    row[column_name] = x[k]
                for k in y:
                    column_name = f"{b.name}.{k}" if b.name else k
                    row[column_name] = y[k]
                row["_tableRows"] = (x, y)
                rows.append(row)
        return Table("", rows)

    def INNER_JOIN(self, a, b, pred):
        return self.CROSS_JOIN(a, b).filter(pred)

    def LEFT_JOIN(self, a, b, pred):
        cp = self.CROSS_JOIN(a, b)
        rows = []
        for aRow in a.rows:
            cpa = [cpr for cpr in cp.rows if aRow in cpr["_tableRows"]]
            match = [cpr for cpr in cpa if pred(cpr)]
            if match:
                rows.extend(match)
            else:
                rows.append(
                    {
                        **{f"{a.name}.{key}": aRow[key] for key in aRow},
                        **{f"{b.name}.{key}": None for key in b.rows[0]},
                    }
                )
        return Table("", rows)

    def RIGHT_JOIN(self, a, b, pred):
        return self.LEFT_JOIN(b, a, pred)

    def LIMIT(self, table, limit):
        return Table(table.name, table.rows[:limit])

    def ORDER_BY(self, table, rel):
        # JS version does sort with lambda taking a and b; Python 3 sort
        # function takes a single argument, so we need to convert
        return Table(table.name, sorted(table.rows, key=functools.cmp_to_key(rel)))

    def HAVING(self, table, pred):
        return table.filter(pred)

    def OFFSET(self, table, offset):
        return Table(table.name, table.rows[offset:])

    def DISTINCT(self, table, columns):
        _distinct = {
            US.join(str(row[col]) for col in columns): row for row in table.rows
        }
        return Table(
            table.name,
            [{col: _distinct[key][col] for col in columns} for key in _distinct],
        )

    def __repr__(self):
        return f"Database({list(self.tables.keys())!r})"


db = Database()
db.CREATE_TABLE("User")
db.INSERT_INTO("User", {"id": 0, "name": "Alice", "age": 25})
db.INSERT_INTO("User", {"id": 1, "name": "Bob", "age": 28})
db.INSERT_INTO("User", {"id": 2, "name": "Charles", "age": 29})
db.INSERT_INTO("User", {"id": 3, "name": "Charles", "age": 35})
db.INSERT_INTO("User", {"id": 4, "name": "Alice", "age": 35})
db.INSERT_INTO("User", {"id": 5, "name": "Jeremy", "age": 28})
db.CREATE_TABLE("Post")
db.INSERT_INTO("Post", {"id": 0, "user_id": 1, "text": "Hello from Bob"})
db.INSERT_INTO("Post", {"id": 1, "user_id": 0, "text": "Hello from Alice"})
db.INSERT_INTO("Post", {"id": 2, "user_id": 10, "text": "Hello from an unknown User"})

User = db.FROM("User")
Post = db.FROM("Post")
# result = db.INNER_JOIN(User, Post, lambda row: row["User.id"] == row["Post.user_id"])
result = db.LEFT_JOIN(User, Post, lambda row: row["User.id"] == row["Post.user_id"])
# result = db.SELECT(
#     result,
#     ["User.age", "User.name", "Post.text"],
#     {"User.name": "Author", "Post.text": "Message"},
# )
# result = db.ORDER_BY(result, lambda a, b: a["User.age"] - b["User.age"])
print(result)


# result = User
# result = db.UPDATE(User, {"name": "CHUCK"}, lambda row: row["id"] == 2)
# result = db.DISTINCT(result, ["name"])
# print(result)
