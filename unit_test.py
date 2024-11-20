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
            tokenize("BEGIN TRANSACTION"),
        ]

        self.testCasesIncorrect = [
            tokenize("COMMTI"),
            tokenize("BEGINE TRANSACTION"),
            tokenize("BEGIN TRANSCATION"),
            tokenize("UPDATE table_name WHERE column_1 = 5 SET column_2 = column_3;"),
            tokenize("UPDATE table_name SET = 5 WHERE column_1 = column_3;"),
            tokenize("SELECT a FROM table_1 ORDRE BY table_1.id"),
            tokenize("SELECT a FROM table_1 ORDER YB table_1.id"),
            tokenize("SELECT a FROM table_1 LIMIT a;"),
            tokenize("SELECT * FROM table_1 NATURAL JOIN table_2 WHERE table_2 table_2.name <<<>>> 'Juan Duado';"),
            tokenize("SELECT a,b FROM table_1 GAMAU JOIN table_2;"),
            tokenize("SELECT arr FORM table_name;"),
            tokenize("SELET * FROM table_name"),
        ]
        self.testAll()

    def testAll(self):
        # Test cases yang seharusnya valid
        for idx, testCase in enumerate(self.testCasesCorrect):
            try:
                assert self.validator.validate(testCase)
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
                assert not self.validator.validate(testCase)
                print(f"Test case {idx + 1} (Incorrect): FAILED - {testCase}")
            except ValueError as e:
                print(f"Test case {idx + 1} (Incorrect): PASSED - {e}")
            finally:
                self.validator.current_state = "Start"

if __name__ == "__main__":
    UnitTest()
