"""A medium-faithful port of https://github.com/weinberg/SQLToy to Python"""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Optional, Set, Tuple


class Table:
    def __init__(self, name: str, rows: Iterable[Dict[str, Any]] = ()):
        self.name = name
        self.rows: Tuple[Dict[str, Any], ...] = tuple(rows)
        self._colnames: Tuple[str, ...] = ()

    def set_colnames(self, colnames: Iterable[str]) -> None:
        self._colnames = tuple(sorted(colnames))

    def colnames(self) -> Tuple[str, ...]:
        if self._colnames:
            return self._colnames
        if not self.rows:
            raise ValueError("Need either rows or manually specified column names")
        return tuple(sorted(self.rows[0].keys()))

    def filter(self, pred: Callable[[Dict[str, Any]], bool]) -> Table:
        return Table(self.name, [row for row in self.rows if pred(row)])

    def __repr__(self) -> str:
        if not self.name:
            return f"Table({list(self.rows)!r})"
        return f"Table({self.name!r}, {list(self.rows)!r})"


class Database:
    def __init__(self) -> None:
        self.tables: Dict[str, Table] = {}

    def CREATE_TABLE(self, name: str, colnames: Iterable[str] = ()) -> Table:
        table = Table(name)
        if colnames:
            table.set_colnames(colnames)
        self.tables[name] = table
        return table

    def DROP_TABLE(self, name: str) -> None:
        del self.tables[name]

    def FROM(self, first_table: str, *rest: str) -> Table:
        match rest:
            case ():
                return self.tables[first_table]
            case _:
                return self.CROSS_JOIN(self.tables[first_table], self.FROM(*rest))

    def SELECT(
        self,
        table: Table,
        columns: Iterable[str],
        aliases: Optional[Dict[str, str]] = None,
    ) -> Table:
        if aliases is None:
            aliases = {}
        return Table(
            table.name,
            [
                {aliases.get(col, col): row[col] for col in columns}
                for row in table.rows
            ],
        )

    def WHERE(self, table: Table, pred: Callable[[Dict[str, Any]], bool]) -> Table:
        return table.filter(pred)

    def INSERT_INTO(self, table_name: str, rows: Iterable[Dict[str, Any]]) -> None:
        table = self.tables[table_name]
        table.rows = (*table.rows, *rows)

    def UPDATE(
        self,
        table: Table,
        set_values: Dict[str, Any],
        pred: Callable[[Dict[str, Any]], bool] = lambda _: True,
    ) -> Table:
        return Table(
            table.name,
            [{**row, **set_values} if pred(row) else row for row in table.rows],
        )

    def CROSS_JOIN(self, a: Table, b: Table) -> Table:
        rows = []
        a_prefix = f"{a.name}." if a.name else ""
        b_prefix = f"{b.name}." if b.name else ""
        for x in a.rows:
            for y in b.rows:
                rows.append(
                    {
                        **{f"{a_prefix}{k}": x[k] for k in a.colnames()},
                        **{f"{b_prefix}{k}": y[k] for k in b.colnames()},
                    }
                )
        return Table("", rows)

    def INNER_JOIN(
        self, a: Table, b: Table, pred: Callable[[Dict[str, Any]], bool]
    ) -> Table:
        return self.CROSS_JOIN(a, b).filter(pred)

    JOIN = INNER_JOIN

    def LEFT_JOIN(
        self, a: Table, b: Table, pred: Callable[[Dict[str, Any]], bool]
    ) -> Table:
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

    def RIGHT_JOIN(
        self, a: Table, b: Table, pred: Callable[[Dict[str, Any]], bool]
    ) -> Table:
        return self.LEFT_JOIN(b, a, pred)

    def LIMIT(self, table: Table, limit: int) -> Table:
        return Table(table.name, table.rows[:limit])

    def ORDER_BY(self, table: Table, rel: Callable[[Dict[str, Any]], Any]) -> Table:
        # Differs from JS version by passing the whole row to the comparator
        return Table(table.name, sorted(table.rows, key=rel))

    def HAVING(self, table: Table, pred: Callable[[Dict[str, Any]], bool]) -> Table:
        return table.filter(pred)

    def OFFSET(self, table: Table, offset: int) -> Table:
        return Table(table.name, table.rows[offset:])

    def DISTINCT(self, table: Table, columns: Iterable[str]) -> Table:
        seen: Set[Tuple[Tuple[str, Any], ...]] = set()
        rows = []
        for row in table.rows:
            view = tuple((col, row[col]) for col in columns)
            if view not in seen:
                seen.add(view)
                rows.append(dict(view))
        return Table(table.name, rows)

    def GROUP_BY(self, table: Table, groupBys: Iterable[str]) -> Table:
        groupRows: Dict[Tuple[Any, ...], list[Dict[str, Any]]] = {}
        for row in table.rows:
            key = tuple(row[col] for col in groupBys)
            if key not in groupRows:
                groupRows[key] = []
            groupRows[key].append(row.copy())
        resultRows = []
        for group in groupRows.values():
            resultRow: Dict[str, Any] = {"_groupRows": group}
            for col in groupBys:
                resultRow[col] = group[0][col]
            resultRows.append(resultRow)
        return Table(table.name, resultRows)

    def _aggregate(
        self,
        table: Table,
        col: str,
        agg_name: str,
        agg: Callable[[Iterable[Dict[str, Any]]], Any],
    ) -> Table:
        grouped = table.rows and "_groupRows" in table.rows[0]
        col_name = f"{agg_name}({col})"
        if not grouped:
            return Table(table.name, [{col_name: agg(table.rows)}])
        rows = []
        for row in table.rows:
            new_row = {}
            for key, value in row.items():
                if key == "_groupRows":
                    new_row[col_name] = agg(value)
                else:
                    new_row[key] = value
            rows.append(new_row)
        return Table(table.name, rows)

    def COUNT(self, table: Table, col: str) -> Table:
        return self._aggregate(table, col, "COUNT", lambda rows: len(list(rows)))

    def MAX(self, table: Table, col: str) -> Table:
        return self._aggregate(
            table, col, "MAX", lambda rows: max(row[col] for row in rows)
        )

    def SUM(self, table: Table, col: str) -> Table:
        return self._aggregate(
            table, col, "SUM", lambda rows: sum(row[col] for row in rows)
        )

    def __repr__(self) -> str:
        return f"Database({list(self.tables.keys())!r})"


def query(
    db: Database,
    select: Iterable[str] = (),
    select_as: Optional[Dict[str, str]] = None,
    distinct: Optional[Iterable[str]] = None,
    from_: Optional[Iterable[str]] = None,
    join: Iterable[Tuple[str, Callable[[Dict[str, Any]], bool]]] = (),
    where: Iterable[Callable[[Dict[str, Any]], bool]] = (),
    group_by: Iterable[str] = (),
    having: Optional[Callable[[Dict[str, Any]], bool]] = None,
    order_by: Optional[Callable[[Dict[str, Any]], Any]] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
) -> Table:
    if from_ is None:
        raise ValueError("Need a FROM clause")
    result = db.FROM(*from_)
    for j in join:
        table_name, pred = j
        result = db.JOIN(result, db.tables[table_name], pred)
    for w in where:
        result = db.WHERE(result, w)
    if group_by:
        result = db.GROUP_BY(result, group_by)
    if having:
        result = db.HAVING(result, having)
    if select:
        result = db.SELECT(result, select, select_as or {})
    if order_by:
        result = db.ORDER_BY(result, order_by)
    if offset:
        result = db.OFFSET(result, offset)
    if limit:
        result = db.LIMIT(result, limit)
    if distinct:
        result = db.DISTINCT(result, distinct)
    return result


def csv(table: Table) -> None:
    print(",".join(table.colnames()))
    for row in table.rows:
        print(",".join(str(val) for val in row.values()))
