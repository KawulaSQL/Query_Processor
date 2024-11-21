from validation import SQLValidator
import re

def tokenize(query):
    return re.findall(r"'.*?'|\".*?\"|[A-Za-z_][A-Za-z0-9_]*|[0-9]+(?:\.[0-9]+)?|[-+]?[0-9]+(?:\.[0-9]+)?|[=><!]+|,|;|\*|\.", query)


class UnitTest:
    def __init__(self): 
        self.validator = SQLValidator()

        self.passed_count = 0
        self.failed_count = 0

        self.testCasesCorrect = [
            tokenize("SELECT * FROM users;"),
            tokenize("SELECT id, name FROM users;"),
            tokenize("SELECT * FROM employee WHERE salary > 1000;"),
            tokenize("UPDATE employee SET salary = 1.05 * salary WHERE salary > 1000;"),
            tokenize("SELECT * FROM student AS s, lecturer AS l WHERE s.lecturer_id = l.id;"),
            tokenize("SELECT * FROM orders WHERE price >= 100;"),
            tokenize("SELECT name FROM users ORDER BY name ASC;"),
            tokenize("SELECT salary FROM employee ORDER BY salary DESC;"),
            tokenize("SELECT * FROM users LIMIT 10;"),
            tokenize("SELECT * FROM products WHERE price < 500 LIMIT 20;"),
            tokenize("SELECT id, name FROM table1 NATURAL JOIN table2;"),
            tokenize("SELECT * FROM orders WHERE price <= 1000 ORDER BY price DESC;"),
            tokenize("UPDATE users SET age = age * 2 WHERE age >= 18;"),
            tokenize("SELECT * FROM users WHERE name = 'John';"),
            tokenize("SELECT * FROM books WHERE title <> 'Unknown';"),
            tokenize("SELECT * FROM employees WHERE salary >= 50000;"),
            tokenize("SELECT * FROM table1 JOIN table2 ON table1.id = table2.id;"),
            tokenize("SELECT * FROM sales WHERE amount < 1000 ORDER BY amount ASC;"),
            tokenize("SELECT id, name FROM customers WHERE id > 100 LIMIT 5;"),
            tokenize("SELECT * FROM users WHERE username <> 'admin';"),
            tokenize("SELECT * FROM orders WHERE order_date >= '2023-01-01';"),
            tokenize("SELECT * FROM employee WHERE dept_id = 1;"),
            tokenize("SELECT id, salary FROM employee ORDER BY salary ASC LIMIT 5;"),
            tokenize("UPDATE employee SET salary = salary * 1.2 WHERE id = 7;"),
            tokenize("SELECT * FROM table1 NATURAL JOIN table2 WHERE table1.id >= 10;"),
        ]

        self.testCasesIncorrect = [
            tokenize("SELET * FROM users;"),
            tokenize("UPDATE employee SET salary = salary * 1.1 salary > 1000;"),
            tokenize("SELECT * WHEN users;"),
            tokenize("SELECT * FROM users WHERE name == 'John';"),
            tokenize("SELECT FROM users WHERE age > 18;"),
            tokenize("SELECT * FROM users ORDER YB name;"),
            tokenize("UPDATE employee SET salary;"),
            tokenize("SELECT * WHERE id > 10;"),
            tokenize("SELECT name, FROM users;"),
            tokenize("SELECT id, name LIMIT 10 FROM users;"),
            tokenize("SELECT * FROM table1 LIMIT -5;"),
            tokenize("SELECT * FROM orders ORDER BY ASC;"),
            tokenize("UPDATE employee SET salary = WHERE id = 5;"),
            tokenize("SELECT * FROM employee WHERE salary !> 1000;"),
            tokenize("SELECT * FROM users WHERE name LIKE '%John%';"),
            tokenize("SELECT * FROM table1 JOIN table2 WHERE table1.id table2.id;"),
            tokenize("SELECT name FROM users GROUP BY age;"),
            tokenize("SELECT * FROM employee ORDER salary;"),
            tokenize("SELECT * FROM users WHERE age <>;"),
            tokenize("SELECT name FROM WHERE id = 1;"),
            tokenize("UPDATE employee SET WHERE id = 5;"),
            tokenize("SELECT * FROM users LIMIT LIMIT;"),
            tokenize("SELECT name AS users WHERE age > 10;"),
            tokenize("SELECT * FROM table1 NATURAL JOIN;"),
            tokenize("UPDATE employee SET salary salary * 1.1 WHERE id = 10;"),
        ]

        self.testAll()

    def testAll(self):
        # Test cases yang seharusnya valid
        for idx, testCase in enumerate(self.testCasesCorrect):
            try:
                result = self.validator.validate(testCase)
                assert result
                print(f"Test case {idx + 1} (Correct): PASSED")
                self.passed_count += 1
            except AssertionError:
                print(f"Test case {idx + 1} (Correct): FAILED - {testCase}")
                self.failed_count += 1
            except ValueError as e:
                print(f"Test case {idx + 1} (Correct): FAILED - {e}")
                self.failed_count += 1
            finally:
                self.validator.current_state = "Start"

        # Test cases yang seharusnya invalid
        for idx, testCase in enumerate(self.testCasesIncorrect):
            try:
                result = self.validator.validate(testCase)
                assert not result
                print(f"Test case {idx + 1} (Incorrect): PASSED")
                self.passed_count += 1
            except AssertionError:
                print(f"Test case {idx + 1} (Incorrect): FAILED - {testCase}")
                self.failed_count += 1
            except ValueError as e:
                print(f"Test case {idx + 1} (Incorrect): PASSED")
                self.passed_count += 1
            finally:
                self.validator.current_state = "Start"

        print(f"\nTotal PASSED: {self.passed_count} ✅")
        print(f"Total FAILED: {self.failed_count} ❌")


if __name__ == "__main__":
    UnitTest()
