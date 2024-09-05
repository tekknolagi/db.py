"""A medium-faithful port of https://github.com/weinberg/SQLToy to Python"""

import functools


class Table:
    def __init__(self, name: str, rows: tuple[dict] = ()):
        self.name = name
        self.rows = tuple(rows)
        self._colnames = ()

    def set_colnames(self, colnames):
        self._colnames = tuple(sorted(colnames))

    def colnames(self):
        if self._colnames:
            return self._colnames
        if not self.rows:
            raise ValueError("Need either rows or manually specified column names")
        return tuple(sorted(self.rows[0].keys()))

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

    def CREATE_TABLE(self, name, colnames=()):
        table = empty_table(name)
        if colnames:
            table.set_colnames(colnames)
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
        return table.filter(pred)

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
                rows.append(
                    {
                        **{f"{a.name}.{k}": x[k] for k in a.colnames()},
                        **{f"{b.name}.{k}": y[k] for k in b.colnames()},
                    }
                )
        return Table("", rows)

    def INNER_JOIN(self, a, b, pred):
        return self.CROSS_JOIN(a, b).filter(pred)

    def LEFT_JOIN(self, a, b, pred):
        rows = []
        empty_b_row = {f"{b.name}.{k}": None for k in b.colnames()}
        for a_row in a.rows:
            added = False
            mangled_a_row = {f"{a.name}.{k}": a_row[k] for k in a.colnames()}
            for b_row in b.rows:
                row = {
                    **mangled_a_row,
                    **{f"{b.name}.{k}": b_row[k] for k in b.colnames()},
                }
                if pred(row):
                    rows.append(row)
                    added = True
            if not added:
                rows.append({**mangled_a_row, **empty_b_row})
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


def csv(table):
    print(",".join(table.colnames()))
    for row in table.rows:
        print(",".join(str(val) for val in row.values()))


db = Database()
db.CREATE_TABLE("User", ["id", "name", "age"])
db.INSERT_INTO("User", {"id": 0, "name": "Alice", "age": 25})
db.INSERT_INTO("User", {"id": 1, "name": "Bob", "age": 28})
db.INSERT_INTO("User", {"id": 2, "name": "Charles", "age": 29})
db.INSERT_INTO("User", {"id": 3, "name": "Charles", "age": 35})
db.INSERT_INTO("User", {"id": 4, "name": "Alice", "age": 35})
db.INSERT_INTO("User", {"id": 5, "name": "Jeremy", "age": 28})
db.CREATE_TABLE("Post", ["id", "user_id", "text"])
db.INSERT_INTO("Post", {"id": 0, "user_id": 1, "text": "Hello from Bob"})
db.INSERT_INTO("Post", {"id": 1, "user_id": 1, "text": "Hello again from Bob"})
# db.INSERT_INTO("Post", {"id": 2, "user_id": 0, "text": "Hello from Alice"})
# db.INSERT_INTO("Post", {"id": 3, "user_id": 10, "text": "Hello from an unknown User"})

User = db.FROM("User")
Post = db.FROM("Post")
# result = db.CROSS_JOIN(User, Post)
# for row in result.rows:
#     print(row)
# result = db.INNER_JOIN(User, Post, lambda row: row["User.id"] == row["Post.user_id"])
result = db.LEFT_JOIN(User, Post, lambda row: row["User.id"] == row["Post.user_id"])
# result = db.SELECT(
#     result,
#     ["User.age", "User.name", "Post.text"],
#     {"User.name": "Author", "Post.text": "Message"},
# )
# result = db.ORDER_BY(result, lambda a, b: a["User.age"] - b["User.age"])
csv(result)


# result = User
# result = db.UPDATE(User, {"name": "CHUCK"}, lambda row: row["id"] == 2)
# result = db.DISTINCT(result, ["name"])
# print(result)
