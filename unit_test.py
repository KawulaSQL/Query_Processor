from validation import SQLValidator
import re

def tokenize(query):
    return re.findall(r"[A-Za-z_][A-Za-z0-9_]*|[=><!]+|[0-9]+|,|;|\*", query)

class TestQuery:
    def __init__(self, query: str):
        self.query = query

class UnitTest:
    def __init__(self):
        self.validator = SQLValidator()
        self.testCasesCorrect = [
            tokenize("SELECT * FROM relation;"),
            # tokenize("SELECT a,b FROM table_name;"),
            # tokenize("SELECT a FROM table_1 JOIN table_2 ON;"),
            # tokenize("SELECT a,b,c FROM table_1 JOIN table_2 ON table_1.id = table_2.id;"),
            # tokenize("SELECT * FROM table_1 NATURAL JOIN table_2;"),
            # tokenize("SELECT a,b,c,d FROM table_1 NATURAL JOIN table_2 WHERE table_2.name = 'Varane Navisse Rayan';"),
            # tokenize("SELECT a,b FROM table_1 JOIN table_2 ON table_1.id = table_2.id WHERE table_2.score <> 4;"),
            # tokenize("SELECT a AS e,c FROM table_1 NATURAL JOIN table_2 ORDER BY table_1.id;"),
            # tokenize("SELECT a AS e FROM table_1"),
            # tokenize("SELECT a AS e,c FROM table_1 LIMIT 7;"),
            # tokenize("UPDATE table_1 SET table_1.score = table_1.score*8 WHERE table_1.score = table_1.score/8;"),
            # tokenize("UPDATE table_name SET column_1 = 5 WHERE column_2 > 10;"),
            # tokenize("UPDATE table_name SET column_1 = column_1 + 1, column_2 = column_2 - 1 WHERE column_3 <= 0;"),
            # tokenize("UPDATE table_name SET column_1 = 5, column_2 = column_2 / 2;"),
            # tokenize("UPDATE table_1 SET column_a = column_b, column_c = column_d WHERE column_a <> column_b;"),
            # tokenize("UPDATE table_1 JOIN table_2 ON table_1.id = table_2.id SET table_1.score = table_2.score * 2;"),
            # tokenize("UPDATE table_1 NATURAL JOIN table_2 SET table_1.name = 'Updated' WHERE table_2.id = 1;"),
            # tokenize("UPDATE table_1 SET score = score + 10 LIMIT 5;"),
            # tokenize("BEGIN TRANSACTION"),
            # tokenize("COMMIT"),
        ]
        self.testCasesIncorrect = [
            # tokenize("COMMTI"),
            # tokenize("BEGINE TRANSACTION"),
            # tokenize("BEGIN TRANSCATION"),
            # tokenize("UPDATE table_name WHERE column_1 = 5 SET column_2 = column_3;"),
            # tokenize("UPDATE table_name SET = 5 WHERE column_1 = column_3;"),
            # tokenize("SELECT a FROM table_1 ORDRE BY table_1.id"),
            # tokenize("SELECT a FROM table_1 ORDER YB table_1.id"),
            # tokenize("SELECT a FROM table_1 LIMIT a;"),
            # tokenize("SELECT * FROM table_1 NATURAL JOIN table_2 WHERE table_2 table_2.name <<<>>> 'Juan Duado';"),
            # tokenize("SELECT a,b FROM table_1 GAMAU JOIN table_2;"),
            # tokenize("SELECT arr FORM table_name;"),
            # tokenize("SELET * FROM table_name"),
            # tokenize("SELECT & FROM table_name;"),
            tokenize("FROM SELECT * users WHERE id = 1;")
        ]
        self.testAll()

    def testAll(self):
        for testCase in self.testCasesCorrect:
            assert self.validator.validate(testCase)
        self.validator.current_state = "Start"
        for testCase in self.testCasesIncorrect:
            try:
                assert not self.validator.validate(testCase)
            except ValueError as e:
                print(e)
                assert False
        print("Success")

if __name__ == "__main__":
    UnitTest()