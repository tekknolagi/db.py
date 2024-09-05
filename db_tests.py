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

    def test_from_with_two_tables_returns_cartesian_product(self):
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

    def test_where_returns_matching_rows(self):
        db = Database()
        table = Table("foo", [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}])
        result = db.WHERE(table, lambda row: row["a"] % 2 == 0)
        self.assertEqual(result.rows, ({"a": 2}, {"a": 4}))

    def test_update_returns_updated_table(self):
        db = Database()
        rows = (
            {"id": 1, "name": "Josh", "department_id": 1, "salary": 50000},
            {"id": 2, "name": "Ruth", "department_id": 2, "salary": 60000},
            {"id": 3, "name": "Greg", "department_id": 5, "salary": 70000},
            {"id": 4, "name": "Pat", "department_id": 1, "salary": 80000},
        )
        table = Table("employee", rows)
        result = db.UPDATE(
            table, {"name": "JOSH", "salary": 10}, lambda row: row["name"] == "Josh"
        )
        self.assertEqual(result.name, table.name)
        self.assertEqual(
            result.rows,
            (
                {"id": 1, "name": "JOSH", "department_id": 1, "salary": 10},
                {"id": 2, "name": "Ruth", "department_id": 2, "salary": 60000},
                {"id": 3, "name": "Greg", "department_id": 5, "salary": 70000},
                {"id": 4, "name": "Pat", "department_id": 1, "salary": 80000},
            ),
        )
        self.assertEqual(table.rows[0]["salary"], 50000)

    def test_update_does_not_modify_table(self):
        db = Database()
        rows = ({"id": 1, "name": "Josh", "department_id": 1, "salary": 50000},)
        table = Table("employee", rows)
        db.UPDATE(
            table, {"name": "JOSH", "salary": 10}, lambda row: row["name"] == "Josh"
        )
        self.assertEqual(table.rows[0]["salary"], 50000)

    def test_cross_join_returns_cartesian_product(self):
        db = Database()
        foo = Table("foo", [{"a": 1}, {"a": 2}])
        bar = Table("bar", [{"b": 1}, {"b": 2}])
        self.assertEqual(
            db.CROSS_JOIN(foo, bar).rows,
            (
                {"foo.a": 1, "bar.b": 1},
                {"foo.a": 1, "bar.b": 2},
                {"foo.a": 2, "bar.b": 1},
                {"foo.a": 2, "bar.b": 2},
            ),
        )

    def test_inner_join_returns_matching_cross_product(self):
        user = Table("user", [{"id": 1, "name": "Alice"}])
        post = Table(
            "post",
            [
                {"id": 1, "user_id": 1, "title": "Hello"},
                {"id": 2, "user_id": 2, "title": "Hello world"},
                {"id": 3, "user_id": 1, "title": "Goodbye world"},
                {"id": 2, "user_id": 3, "title": "Hello world again"},
            ],
        )
        db = Database()
        result = db.JOIN(user, post, lambda row: row["user.id"] == row["post.user_id"])
        self.assertEqual(
            result.rows,
            (
                {
                    "post.id": 1,
                    "post.title": "Hello",
                    "post.user_id": 1,
                    "user.id": 1,
                    "user.name": "Alice",
                },
                {
                    "post.id": 3,
                    "post.title": "Goodbye world",
                    "post.user_id": 1,
                    "user.id": 1,
                    "user.name": "Alice",
                },
            ),
        )

    def test_left_join_returns_matching_rows_on_right(self):
        employee = Table(
            "employee",
            [
                {"id": 1, "name": "Alice", "department_id": 1},
                {"id": 2, "name": "Bob", "department_id": 2},
            ],
        )
        department = Table(
            "department",
            [
                {"id": 1, "title": "Accounting"},
                {"id": 2, "title": "Engineering"},
            ],
        )
        db = Database()
        result = db.LEFT_JOIN(
            employee,
            department,
            lambda row: row["employee.department_id"] == row["department.id"],
        )
        self.assertEqual(
            result.rows,
            (
                {
                    "department.id": 1,
                    "department.title": "Accounting",
                    "employee.department_id": 1,
                    "employee.id": 1,
                    "employee.name": "Alice",
                },
                {
                    "department.id": 2,
                    "department.title": "Engineering",
                    "employee.department_id": 2,
                    "employee.id": 2,
                    "employee.name": "Bob",
                },
            ),
        )

    def test_left_join_fills_in_null_for_non_matching_rows(self):
        employee = Table(
            "employee",
            [
                {"id": 1, "name": "Alice", "department_id": 100},
                {"id": 2, "name": "Bob", "department_id": 2},
            ],
        )
        department = Table(
            "department",
            [
                {"id": 1, "title": "Accounting"},
                {"id": 2, "title": "Engineering"},
            ],
        )
        db = Database()
        result = db.LEFT_JOIN(
            employee,
            department,
            lambda row: row["employee.department_id"] == row["department.id"],
        )
        self.assertEqual(
            result.rows,
            (
                {
                    "department.id": None,
                    "department.title": None,
                    "employee.department_id": 100,
                    "employee.id": 1,
                    "employee.name": "Alice",
                },
                {
                    "department.id": 2,
                    "department.title": "Engineering",
                    "employee.department_id": 2,
                    "employee.id": 2,
                    "employee.name": "Bob",
                },
            ),
        )

    def test_right_join_returns_matching_rows_on_left(self):
        employee = Table(
            "employee",
            [
                {"id": 1, "name": "Alice", "department_id": 1},
                {"id": 2, "name": "Bob", "department_id": 2},
            ],
        )
        department = Table(
            "department",
            [
                {"id": 1, "title": "Accounting"},
                {"id": 2, "title": "Engineering"},
            ],
        )
        db = Database()
        result = db.RIGHT_JOIN(
            employee,
            department,
            lambda row: row["employee.department_id"] == row["department.id"],
        )
        self.assertEqual(
            result.rows,
            (
                {
                    "department.id": 1,
                    "department.title": "Accounting",
                    "employee.department_id": 1,
                    "employee.id": 1,
                    "employee.name": "Alice",
                },
                {
                    "department.id": 2,
                    "department.title": "Engineering",
                    "employee.department_id": 2,
                    "employee.id": 2,
                    "employee.name": "Bob",
                },
            ),
        )

    def test_right_join_fills_in_null_for_non_matching_rows(self):
        employee = Table(
            "employee",
            [
                {"id": 1, "name": "Alice", "department_id": 100},
                {"id": 2, "name": "Bob", "department_id": 2},
                {"id": 3, "name": "Charles", "department_id": 2},
            ],
        )
        department = Table(
            "department",
            [
                {"id": 1, "title": "Accounting"},
                {"id": 2, "title": "Engineering"},
            ],
        )
        db = Database()
        result = db.RIGHT_JOIN(
            employee,
            department,
            lambda row: row["employee.department_id"] == row["department.id"],
        )
        self.assertEqual(
            result.rows,
            (
                {
                    "department.id": 1,
                    "department.title": "Accounting",
                    "employee.department_id": None,
                    "employee.id": None,
                    "employee.name": None,
                },
                {
                    "department.id": 2,
                    "department.title": "Engineering",
                    "employee.department_id": 2,
                    "employee.id": 2,
                    "employee.name": "Bob",
                },
                {
                    "department.id": 2,
                    "department.title": "Engineering",
                    "employee.department_id": 2,
                    "employee.id": 3,
                    "employee.name": "Charles",
                },
            ),
        )

    def tests_limit_returns_empty_table(self):
        db = Database()
        table = Table("foo", [])
        result = db.LIMIT(table, 1)
        self.assertEqual(result.rows, ())

    def tests_limit_returns_no_more_than_limit(self):
        db = Database()
        table = Table("foo", [{"a": 1}, {"a": 2}, {"a": 3}])
        result = db.LIMIT(table, 2)
        self.assertEqual(result.rows, ({"a": 1}, {"a": 2}))


if __name__ == "__main__":
    unittest.main()
