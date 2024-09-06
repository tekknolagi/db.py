"""A medium-faithful port of https://github.com/weinberg/SQLToy to Python"""


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


class Database:
    def __init__(self):
        self.tables = {}

    def CREATE_TABLE(self, name, colnames=()):
        table = Table(name)
        if colnames:
            table.set_colnames(colnames)
        self.tables[name] = table
        return table

    def DROP_TABLE(self, name):
        del self.tables[name]

    def FROM(self, first_table, *rest):
        match rest:
            case ():
                return self.tables[first_table]
            case _:
                return self.CROSS_JOIN(self.tables[first_table], self.FROM(*rest))

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

    def INSERT_INTO(self, table_name, rows):
        table = self.tables[table_name]
        table.rows = (*table.rows, *rows)

    def UPDATE(self, table, set, pred=lambda _: True):
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
        return Table(f"{a.name}_{b.name}", rows)

    def INNER_JOIN(self, a, b, pred):
        return self.CROSS_JOIN(a, b).filter(pred)

    JOIN = INNER_JOIN

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
        return Table(f"{a.name}_{b.name}", rows)

    def RIGHT_JOIN(self, a, b, pred):
        return self.LEFT_JOIN(b, a, pred)

    def LIMIT(self, table, limit):
        return Table(table.name, table.rows[:limit])

    def ORDER_BY(self, table, rel):
        # Differs from JS version by passing the whole row to the comparator
        return Table(table.name, sorted(table.rows, key=rel))

    def HAVING(self, table, pred):
        return table.filter(pred)

    def OFFSET(self, table, offset):
        return Table(table.name, table.rows[offset:])

    def DISTINCT(self, table, columns):
        US = "\x1f"  # Unit Separator
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
