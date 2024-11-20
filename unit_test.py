from validation import SQLValidator
import re

def tokenize(query):
    return re.findall(r"[A-Za-z_][A-Za-z0-9_]*|[=><!]+|[0-9]+|,|;|\*", query)

class UnitTest:
    def __init__(self): 
        self.validator = SQLValidator()
        self.testCasesCorrect = [
            tokenize("SELECT * FROM relation;"),
            tokenize("SELECT ab FROM table_name;"),
            tokenize("SELECT column1, column2 FROM table_name WHERE column3 > 10;"),
            tokenize("BEGIN TRANSACTION;"),
            tokenize("UPDATE employee SET salary = salary * 1.1 WHERE id = 5;"),
            tokenize("SELECT name FROM student WHERE grade >= 85;"),
            tokenize("SELECT id, name FROM table1 NATURAL JOIN table2;"),
            tokenize("SELECT * FROM orders ORDER BY price DESC;"),
            tokenize("SELECT * FROM users LIMIT 5;"),
            tokenize("SELECT * FROM products WHERE price < 1000 ORDER BY price ASC LIMIT 20;"),
            tokenize("SELECT * FROM employee AS e, department AS d WHERE e.dept_id = d.id;"),
            tokenize("SELECT column1 AS alias1 FROM table_name;"),
        ]

        self.testCasesIncorrect = [
            tokenize("COMMITE"),
            tokenize("BEGIN TRANSACTION WITHOUT ENDING;"),
            tokenize("BEGIN TRANSCATION"),
            tokenize("UPDATE table_name WHERE column_1 = 5 SET column_2 = column_3;"),
            tokenize("UPDATE table_name SET = 5 WHERE column_1 = column_3;"),
            tokenize("SELECT a FROM table_1 ORDRE BY table_1.id;"),
            tokenize("SELECT a FROM table_1 ORDER YB table_1.id;"),
            tokenize("SELECT a FROM table_1 LIMIT a;"),
            tokenize("SELECT * FROM table_1 NATURAL JOIN table_2 WHERE table_2 table_2.name <<<>>> 'Juan Duado';"),
            tokenize("SELECT a,b FROM table_1 GAMAU JOIN table_2;"),
            tokenize("SELECT arr FORM table_name;"),
            tokenize("SELET * FROM table_name;"),
            tokenize("SELECT * FROM users WHERE name == 'John';"),
            tokenize("UPDATE employee SET salary salary * 1.1 WHERE id = 5;"),
            tokenize("SELECT * WHERE grade >= 85;"),
            tokenize("SELECT id, name, FROM table;"),
            tokenize("SELECT FROM table WHERE id = 1;"), 
            tokenize("SELECT column1 FROM table1 NATURAL JOIN;"),
            tokenize("SELECT column1 FROM table1 ORDER BY column1 LIMIT -5;"),
            tokenize("SELECT * FROM table1 LIMIT 5.5;"),
        ]
        self.testAll()

    def testAll(self):
    # Test cases yang seharusnya valid
        for idx, testCase in enumerate(self.testCasesCorrect):
            try:
                result = self.validator.validate(testCase)
                assert result
                print(f"Test case {idx + 1} (Correct): PASSED")
            except AssertionError:
                print(f"Test case {idx + 1} (Correct): FAILED - {testCase}")
            except ValueError as e:
                print(f"Test case {idx + 1} (Correct): FAILED - {e}")
            finally:
                self.validator.current_state = "Start"

        # Test cases yang seharusnya invalid
        for idx, testCase in enumerate(self.testCasesIncorrect):
            try:
                result = self.validator.validate(testCase)
                assert not result
                print(f"Test case {idx + 1} (Incorrect): PASSED")
            except AssertionError:
                print(f"Test case {idx + 1} (Incorrect): FAILED - {testCase}")
            except ValueError as e:
                print(f"Test case {idx + 1} (Incorrect): PASSED")
            finally:
                self.validator.current_state = "Start"


if __name__ == "__main__":
    UnitTest()
