import unittest
from db import Database, Table

__import__("sys").modules["unittest.util"]._MAX_LENGTH = 999999999


class DatabaseTests(unittest.TestCase):
    def test_create_table(self):
        db = Database()
        self.assertNotIn("foo", db.tables)
        db.CREATE_TABLE("foo")
        self.assertIn("foo", db.tables)

    def test_create_table_creates_empty_table(self):
        db = Database()
        table = db.CREATE_TABLE("foo")
        self.assertEqual(table.rows, ())

    def test_create_table_sets_colnames(self):
        db = Database()
        table = db.CREATE_TABLE("foo", ["a", "b"])
        self.assertEqual(table.colnames(), ("a", "b"))

    def test_insert_into_adds_row(self):
        db = Database()
        table = db.CREATE_TABLE("stories")
        row = {
            "id": 1,
            "name": "The Elliptical Machine that ate Manhattan",
            "author_id": 1,
        }
        db.INSERT_INTO("stories", [row])
        self.assertIn(row, table.rows)

    def test_insert_into_adds_rows(self):
        db = Database()
        table = db.CREATE_TABLE("stories")
        rows = [
            {
                "id": 1,
                "name": "The Elliptical Machine that ate Manhattan",
                "author_id": 1,
            },
            {"id": 2, "name": "Queen of the Bats", "author_id": 2},
        ]
        db.INSERT_INTO("stories", rows)
        self.assertIn(rows[0], table.rows)
        self.assertIn(rows[1], table.rows)

    def test_insert_into_appends_row(self):
        db = Database()
        table = db.CREATE_TABLE("stories")
        rows = [
            {
                "id": 1,
                "name": "The Elliptical Machine that ate Manhattan",
                "author_id": 1,
            },
            {"id": 2, "name": "Queen of the Bats", "author_id": 2},
        ]
        db.INSERT_INTO("stories", rows)
        new_row = {"id": 4, "name": "Something", "author_id": 5}
        db.INSERT_INTO("stories", [new_row])
        self.assertIn(rows[0], table.rows)
        self.assertIn(rows[1], table.rows)
        self.assertIn(new_row, table.rows)

    def test_drop_table_removes_table(self):
        db = Database()
        db.CREATE_TABLE("foo")
        self.assertIn("foo", db.tables)
        db.DROP_TABLE("foo")
        self.assertNotIn("foo", db.tables)

    def test_from_with_no_tables_raises(self):
        db = Database()
        with self.assertRaises(TypeError):
            db.FROM()

    def test_from_with_one_table_returns_table(self):
        db = Database()
        table = db.CREATE_TABLE("foo")
        result = db.FROM("foo")
        self.assertEqual(result, table)

    def test_from_with_two_tables_returns_cross_join(self):
        db = Database()
        db.CREATE_TABLE("foo")
        db.INSERT_INTO("foo", [{"a": 1}, {"a": 2}])
        db.CREATE_TABLE("bar")
        db.INSERT_INTO("bar", [{"b": 1}, {"b": 2}])
        self.assertEqual(
            db.FROM("foo", "bar").rows,
            (
                {"foo.a": 1, "bar.b": 1},
                {"foo.a": 1, "bar.b": 2},
                {"foo.a": 2, "bar.b": 1},
                {"foo.a": 2, "bar.b": 2},
            ),
        )

    def test_select_returns_table_with_no_columns(self):
        db = Database()
        table = Table("foo", [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        result = db.SELECT(table, [])
        self.assertEqual(result.rows, ({}, {}))

    def test_select_returns_table_with_given_columns(self):
        db = Database()
        table = Table("foo", [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        result = db.SELECT(table, ["a", "c"])
        self.assertEqual(result.rows, ({"a": 1, "c": 3}, {"a": 4, "c": 6}))

    def test_select_returns_table_with_aliases(self):
        db = Database()
        table = Table("foo", [{"a": 1, "b": 2, "c": 3}, {"a": 4, "b": 5, "c": 6}])
        result = db.SELECT(table, ["a", "c"], {"a": "x"})  # a as x
        self.assertEqual(result.rows, ({"x": 1, "c": 3}, {"x": 4, "c": 6}))


if __name__ == "__main__":
    unittest.main()
