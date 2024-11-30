import unittest
from query_processor import QueryProcessor

class TestQuery(unittest.TestCase):
    def setUp(self):
        base_path = "./Storage_Manager/storage"
        self.query_processor = QueryProcessor(base_path)

        self.select_true = [
            "SELECT * FROM users;",
            "SELECT id, name FROM users;",
            "SELECT * FROM employee WHERE salary > 1000;",
            "UPDATE employee SET salary = 1.05 * salary WHERE salary > 1000;",
            "SELECT * FROM student AS s, lecturer AS l WHERE s.lecturer_id = l.id;",
            "SELECT * FROM orders WHERE price >= 100;",
            "SELECT name FROM users ORDER BY name ASC;",
            "SELECT salary FROM employee ORDER BY salary DESC;",
            "SELECT * FROM users LIMIT 10;",
            "SELECT * FROM products WHERE price < 500 LIMIT 20;",
            "SELECT id, name FROM table1 NATURAL JOIN table2;",
            "SELECT * FROM orders WHERE price <= 1000 ORDER BY price DESC;",
            "UPDATE users SET age = age * 2 WHERE age >= 18;",
            "SELECT * FROM users WHERE name = 'John';",
            "SELECT * FROM books WHERE title <> 'Unknown';",
            "SELECT * FROM employees WHERE salary >= 50000;",
            "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id;",
            "SELECT * FROM sales WHERE amount < 1000 ORDER BY amount ASC;",
            "SELECT id, name FROM customers WHERE id > 100 LIMIT 5;",
            "SELECT * FROM users WHERE username <> 'admin';",
            "SELECT * FROM orders WHERE order_date >= '2023-01-01';",
            "SELECT * FROM employee WHERE dept_id = 1;",
            "SELECT id, salary FROM employee ORDER BY salary ASC LIMIT 5;",
            "UPDATE employee SET salary = salary * 1.2 WHERE id = 7;",
            "SELECT * FROM table1 NATURAL JOIN table2 WHERE table1.id >= 10;",
        ]

        self.select_false = [
            "SELET * FROM users;",
            "UPDATE employee SET salary = salary * 1.1 salary > 1000;",
            "SELECT * WHEN users;",
            "SELECT * FROM users WHERE name == 'John';",
            "SELECT FROM users WHERE age > 18;",
            "SELECT * FROM users ORDER YB name;",
            "UPDATE employee SET salary;",
            "SELECT * WHERE id > 10;",
            "SELECT name, FROM users;",
            "SELECT id, name LIMIT 10 FROM users;",
            "SELECT * FROM table1 LIMIT -5;",
            "SELECT * FROM orders ORDER BY ASC;",
            "UPDATE employee SET salary = WHERE id = 5;",
            "SELECT * FROM employee WHERE salary !> 1000;",
            "SELECT * FROM users WHERE name LIKE '%John%';",
            "SELECT * FROM table1 JOIN table2 WHERE table1.id table2.id;",
            "SELECT name FROM users GROUP BY age;",
            "SELECT * FROM employee ORDER salary;",
            "SELECT * FROM users WHERE age <>;",
            "SELECT name FROM WHERE id = 1;",
            "UPDATE employee SET WHERE id = 5;",
            "SELECT * FROM users LIMIT LIMIT;",
            "SELECT name AS users WHERE age > 10;",
            "SELECT * FROM table1 NATURAL JOIN;",
            "UPDATE employee SET salary salary * 1.1 WHERE id = 10;",
        ]

    def test_select_true(self):
        for query in self.select_true:
            result = self.query_processor.process_query(query)

            self.assertTrue(result, f"Expected True from query: {query}")

    def test_select_false(self):
        for query in self.select_false:
            result = self.query_processor.process_query(query)

            self.assertFalse(result, f"Expected False from query: {query}")

if __name__ == '__main__':
    unittest.main()