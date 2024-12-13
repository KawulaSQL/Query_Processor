import unittest
import shutil
import sys
import io

from QueryProcessor import QueryProcessor

class SuppressPrints:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = io.StringIO()

    def __exit__(self, exc_type, exc_value, traceback):
        sys.stdout = self._original_stdout

class TestQuery(unittest.TestCase):
    def setUp(self):
        # Duplicates the test db so it wont be overwritten
        shutil.copytree('./db-test', 'db-test-copy', dirs_exist_ok=True)

        base_path = "./db-test-copy"
        self.query_processor = QueryProcessor(base_path)

        self.read_case = [
            "SELECT * FROM student;", 
            "SELECT id, name FROM student;", 
            "SELECT * FROM student LIMIT 5;",
            "SELECT * FROM student LIMIT 0;",
            "SELECT * FROM student LIMIT 100;",
            "SELECT * FROM student WHERE total_cred >= 500;", 
            "SELECT * FROM student WHERE id < 100;", 
            "SELECT * FROM student ORDER BY total_cred;",
        ]

        self.write_case = [
            "UPDATE student SET total_cred = 114;",
            "UPDATE student SET age = age * 2 WHERE age <= 18;",

            "CREATE TABLE test1 (int int, float float, char char, varchar varchar(250));",

            "INSERT INTO test1 VALUES (1, 1.5, 'aaa', 'bbb');",

            "DELETE FROM test1;",

            "DROP TABLE test1;",
        ]

        self.invalid_case = [
            "SELECT FROM;",
            "SELECT none FROM student;",
            "SELECT * FROM none;",
            "SELECT * FROM student AS;",
            "SELECT * FROM student LIMIT;",
            "SELECT * FROM WHERE total_cred >= 500;", 
            "SELECT * FROM student, none;",
            "SELECT * FROM student AS s, lecturer AS s WHERE s.id = s.id;", 
            "SELECT name FROM student ORDER BY;"

            "CREATE TABLE test2;",

            "INSERT INTO student;",

            "DELETE student;",

            "DROP FROM student;",
        ]

    def tearDown(self):
        shutil.rmtree('./db-test-copy', ignore_errors=True)

    def test_read(self):
        with SuppressPrints():
            for query in self.read_case:
                result = self.query_processor.process_query(query)

                self.assertTrue(result.status=="success", f"Expected True from query: {query}")

    def test_write(self):
        with SuppressPrints():
            for query in self.write_case:
                result = self.query_processor.process_query(query)

                self.assertTrue(result.status == "success", f"Expected True from query: {query}")

    def test_invalid(self):
        with SuppressPrints():
            for query in self.invalid_case:
                result = self.query_processor.process_query(query)

                self.assertTrue(result.status == "error", f"Expected False from query: {query}")

if __name__ == '__main__':
    unittest.main()