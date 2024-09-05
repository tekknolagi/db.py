import functools

database = {}


def init():
    global database
    database = {"tables": {}}


def empty_table(name=""):
    return {"name": name, "rows": []}


def CREATE_TABLE(name):
    table = empty_table(name)
    database["tables"][name] = table
    return table


def DROP_TABLE(name):
    del database["tables"][name]


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
    result = empty_table()
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


def LEFT_JOIN(a, b, pred):
    cp = CROSS_JOIN(a, b)
    result = empty_table()
    for aRow in a["rows"]:
        cpa = [cpr for cpr in cp["rows"] if aRow in cpr["_tableRows"]]
        match = [cpr for cpr in cpa if pred(cpr)]
        if match:
            result["rows"].extend(match)
        else:
            aValues = {}
            bValues = {}
            for key in aRow:
                aValues[a["name"] + "." + key] = aRow[key]
            for key in b["rows"][0]:
                bValues[b["name"] + "." + key] = None
            result["rows"].append({**aValues, **bValues})
    return result


def RIGHT_JOIN(a, b, pred):
    return LEFT_JOIN(b, a, pred)


def LIMIT(table, limit):
    return {"name": table["name"], "rows": table["rows"][:limit]}


def ORDER_BY(table, rel):
    return {
        "name": table["name"],
        # JS version does sort with lambda taking a and b; Python 3 sort
        # function takes a single argument, so we need to convert
        "rows": sorted(table["rows"], key=functools.cmp_to_key(rel)),
    }


def HAVING(table, pred):
    return {"name": table["name"], "rows": [row for row in table["rows"] if pred(row)]}


def OFFSET(table, offset):
    return {"name": table["name"], "rows": table["rows"][offset:]}


US = "\x1f"  # Unit Separator

def DISTINCT(table, columns):
    _distinct = {}
    for row in table["rows"]:
        key = US.join(str(row[col]) for col in columns)
        _distinct[key] = row
    newRows = []
    for key in _distinct:
        newRow = {}
        for col in columns:
            newRow[col] = _distinct[key][col]
        newRows.append(newRow)
    return {"name": table["name"], "rows": newRows}


init()
CREATE_TABLE("User")
INSERT_INTO("User", {"id": 0, "name": "Alice", "age": 25})
INSERT_INTO("User", {"id": 1, "name": "Bob", "age": 28})
INSERT_INTO("User", {"id": 2, "name": "Charles", "age": 29})
INSERT_INTO("User", {"id": 3, "name": "Charles", "age": 35})
INSERT_INTO("User", {"id": 4, "name": "Alice", "age": 35})
INSERT_INTO("User", {"id": 5, "name": "Jeremy", "age": 28})
CREATE_TABLE("Post")
INSERT_INTO("Post", {"id": 0, "user_id": 1, "text": "Hello from Bob"})
INSERT_INTO("Post", {"id": 1, "user_id": 0, "text": "Hello from Alice"})

User = FROM("User")
# Post = FROM("Post")
# result = INNER_JOIN(User, Post, lambda row: row["User.id"] == row["Post.user_id"])
# result = SELECT(
#     result,
#     ["User.age", "User.name", "Post.text"],
#     {"User.name": "Author", "Post.text": "Message"},
# )
# result = ORDER_BY(result, lambda a, b: a["User.age"] - b["User.age"])
# print(result)


result = DISTINCT(User, ["age"])
print(result)
