import re

class SQLValidator:
    def __init__(self):
        # Daftar kata kunci SQL untuk validasi
        self.SQL_KEYWORDS = {"SELECT", "FROM", "WHERE", "JOIN", "AS", "NATURAL", "ON", "UPDATE", "BEGIN", "COMMIT", "SET"}
        
        # State transitions
        self.transitions = {
            "Start": {"SELECT": "SELECT", "UPDATE": "UPDATE", "BEGIN": "BEGIN TRANSACTIONS"},
            "SELECT": {"*": "attribute", "attribute": "attribute", "FROM": "FROM"},
            "attribute": {"AS": "AS alias", ",": "attribute", "FROM": "FROM", "WHERE": "WHERE"},
            "AS alias": {"attribute": "attribute", "FROM": "FROM"},
            "FROM": {"relation": "relation"},
            "relation": {"AS": "AS relation", "WHERE": "WHERE", "NATURAL": "NATURAL JOIN", "JOIN": "JOIN", ";": "Finish"},
            "JOIN": {"relation": "relation ON"},
            "relation ON": {"ON": "ON condition"},
            "ON condition": {"attribute": "condition"},
            "condition": {"operator": "condition value"},
            "condition value": {"value": "WHERE"},
            "WHERE": {"attribute": "condition"},
            "operator": {"attribute": "condition", "value": "value"},
            "value": {";": "Finish"},
            "UPDATE": {"relation": "SET"},
            "SET": {"attribute=value": "WHERE"},
            "BEGIN TRANSACTIONS": {";": "Finish", "COMMIT": "COMMIT"},
            "COMMIT": {";": "Finish"},
        }
        self.current_state = "Start"

    def validate(self, tokens):
        for token in tokens:
            if self.current_state == "Finish":
                break
            if token in self.transitions.get(self.current_state, {}):
                self.current_state = self.transitions[self.current_state][token]
            elif re.match(r"[=><!]+", token):
                self.current_state = "operator"
            elif re.match(r"[0-9]+", token):
                self.current_state = "value"
            elif re.match(r"[A-Za-z_][A-Za-z0-9_]*", token):
                if token in self.SQL_KEYWORDS:
                    raise ValueError(f"Unexpected keyword '{token}' in state '{self.current_state}'")
                if self.current_state == "FROM":
                    self.current_state = "relation"
                elif self.current_state in ["WHERE", "ON condition"]:
                    self.current_state = "condition"
                else:
                    self.current_state = "attribute"
            else:
                raise ValueError(f"Invalid token '{token}' in state '{self.current_state}'")

        if self.current_state != "Finish":
            raise ValueError("Query did not reach a valid end state")
        return True

def tokenize(query):
    # Tokenizer untuk memisahkan query menjadi token
    return re.findall(r"[A-Za-z_][A-Za-z0-9_]*|[=><!]+|[0-9]+|,|;|\*", query)

# Contoh penggunaan
# query = "SELECT * FROM relation;"
# tokens = tokenize(query)
# print("Tokens:", tokens)

# validator = SQLValidator()

# try:
#     validator.validate(tokens)
#     print("Query is valid!")
# except ValueError as e:
#     print(f"Query validation failed: {e}")
