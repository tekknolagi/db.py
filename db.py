database = {}


def init():
    global database
    database = {"tables": {}}


def CREATE_TABLE(name):
    table = {"name": name, "rows": []}
    database["tables"][name] = table
    return table


def FROM(*table_names):
    assert len(table_names) > 0
    match table_names:
        case (table_name,):
            return database["tables"][table_name]
        case (table_name, *rest):
            return CROSS_JOIN(database["tables"][table_name], FROM(*rest))


def SELECT(table, columns, aliases=None):
    if aliases is None:
        aliases = {}
    newrows = []
    colnames = {}
    for col in columns:
        colnames[col] = aliases.get(col, col)
    for row in table["rows"]:
        newrow = {}
        for col in columns:
            newrow[colnames[col]] = row[col]
        newrows.append(newrow)
    return {"name": table["name"], "rows": newrows}


def WHERE(table, pred):
    return {"name": table["name"], "rows": [row for row in table["rows"] if pred(row)]}


def INSERT_INTO(table_name, *rows):
    table = database["tables"][table_name]
    table["rows"].extend(rows)


def CROSS_JOIN(a, b):
    result = {"name": "", "rows": []}
    for x in a["rows"]:
        for y in b["rows"]:
            row = {}
            for k in x:
                column_name = a["name"] + "." + k if a["name"] else k
                row[column_name] = x[k]
            for k in y:
                column_name = b["name"] + "." + k if b["name"] else k
                row[column_name] = y[k]
            row["_tableRows"] = (x, y)
            result["rows"].append(row)
    return result


def INNER_JOIN(a, b, pred):
    return {"name": "", "rows": [row for row in CROSS_JOIN(a, b)["rows"] if pred(row)]}


init()
CREATE_TABLE("User")
INSERT_INTO("User", {"id": 0, "name": "Alice", "age": 25})
INSERT_INTO("User", {"id": 1, "name": "Bob", "age": 28})
INSERT_INTO("User", {"id": 2, "name": "Charles", "age": 29})
CREATE_TABLE("Post")
INSERT_INTO("Post", {"id": 0, "user_id": 1, "text": "Hello from Bob"})
INSERT_INTO("Post", {"id": 1, "user_id": 0, "text": "Hello from Alice"})

User = FROM("User")
Post = FROM("Post")
result = INNER_JOIN(User, Post, lambda row: row["User.id"] == row["Post.user_id"])
result = SELECT(
    result, ["User.name", "Post.text"], {"User.name": "Author", "Post.text": "Message"}
)
print(result)
