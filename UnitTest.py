import unittest
import shutil

from QueryProcessor import QueryProcessor

class TestQuery(unittest.TestCase):
    def setUp(self):
        # Duplicates the test db so it wont be overwritten
        shutil.copytree('./db-test', 'db-test-copy', dirs_exist_ok=True)

        base_path = "./db-test-copy"
        self.query_processor = QueryProcessor(base_path)

        self.read_case = [
            "SELECT * FROM student;", 
            "SELECT id, name FROM student;", 
            "SELECT * FROM student, instructor;",
            "SELECT * FROM student LIMIT 5;",
            "SELECT * FROM instructor WHERE salary > 1000;", 
            "SELECT * FROM student WHERE total_cred >= 500;", 
            "SELECT * FROM instructor WHERE name = 'A';", 
            "SELECT * FROM student WHERE id < 100;", 
            "SELECT * FROM instructor WHERE id <= 25;", 
            "SELECT * FROM student, lecturer;",
            "SELECT * FROM student AS s, lecturer AS l WHERE s.id = l.id;", 
            "SELECT * FROM student ORDER BY total_cred;",
            "SELECT name FROM student ORDER BY name ASC;",
            "SELECT * FROM instructor ORDER BY salary DESC;",
            "SELECT * FROM student NATURAL JOIN instructor;",
            "SELECT * FROM student JOIN instructor ON student.id = instructor.id;",
            "SELECT id, dept_name FROM student AS s JOIN instructor AS i ON s.id <> i.id WHERE i.salary <= 10000 ORDER BY DESC LIMIT 15;",
        ]

        self.write_case = [
            "UPDATE student SET total_cred = 114;",
            "UPDATE instructor SET salary = 0.95 * salary;",
            "UPDATE instructor SET salary = 1.05 * salary WHERE salary > 1000;",
            "UPDATE instructor SET salary = salary * 1.2 WHERE id = 7;",
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
            "SELECT * FROM student instructor;",
            "SELECT * FROM student LIMIT;",
            "SELECT * FROM instructor WHERE salary 1000;", 
            "SELECT * FROM WHERE total_cred >= 500;", 
            "SELECT * FROM student, none;",
            "SELECT * FROM student AS s, lecturer AS s WHERE s.id = s.id;", 
            "SELECT name FROM student ORDER BY;"
            "SELECT * FROM none NATURAL JOIN instructor;",
            "SELECT * FROM student JOIN instructor ON student.id = none;",

            "CREATE TABLE test2;",

            "INSERT INTO student;",

            "DELETE student;",

            "DROP FROM student;",
        ]

    def tearDown(self):
        shutil.rmtree('./db-test-copy', ignore_errors=True)

    def test_read(self):
        for query in self.read_case:
            result = self.query_processor.process_query(query)

            self.assertTrue(result, f"Expected True from query: {query}")

    def test_write(self):
        for query in self.write_case:
            result = self.query_processor.process_query(query)

            self.assertTrue(result, f"Expected True from query: {query}")

    def test_invalid(self):
        for query in self.invalid_case:
            result = self.query_processor.process_query(query)

            self.assertFalse(result, f"Expected False from query: {query}")

if __name__ == '__main__':
    unittest.main()