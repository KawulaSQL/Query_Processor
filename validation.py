import re

class SQLValidator:
    def __init__(self):
        # Daftar kata kunci SQL untuk validasi
        self.SQL_KEYWORDS = {"SELECT", "FROM", "WHERE", "JOIN", "AS", "NATURAL", "ON", "UPDATE", "COMMIT", "SET", "ORDER", "BY", "LIMIT"}

        # State transitions
        self.transitions = {
            "Start": {"SELECT": "SELECT", "UPDATE": "UPDATE", "BEGIN": "BEGIN TRANSACTION"},
            "SELECT": {"*": "attribute", "attribute": "attribute", "FROM": "FROM"},
            "attribute": {"AS": "AS alias", ",": "attribute", "FROM": "FROM", "WHERE": "WHERE", "ORDER": "ORDER BY", "LIMIT": "LIMIT"},
            "AS alias": {"attribute": "attribute", "FROM": "FROM"},
            "FROM": {"relation": "relation"},
            "relation": {"AS": "AS relation", "WHERE": "WHERE", "NATURAL": "NATURAL JOIN", "JOIN": "JOIN", ";": "Finish", "ORDER": "ORDER BY", "LIMIT": "LIMIT"},
            "JOIN": {"relation": "relation ON"},
            "relation ON": {"ON": "ON condition"},
            "ON condition": {"attribute": "condition"},
            "condition": {"operator": "condition value"},
            "condition value": {"value": "WHERE", ";": "Finish"},
            "WHERE": {"attribute": "condition", "ORDER": "ORDER BY", "LIMIT": "LIMIT", ";": "Finish"},
            "operator": {"attribute": "condition", "value": "value"},
            "value": {";": "Finish", "ORDER": "ORDER BY", "LIMIT": "LIMIT"},
            "UPDATE": {"relation": "SET"},
            "SET": {"attribute=value": "WHERE"},
            "BEGIN TRANSACTION": {";": "Finish", "COMMIT": "COMMIT"},
            "COMMIT": {";": "Finish"},
            "ORDER BY": {"attribute": "ORDER direction"},
            "ORDER direction": {"ASC": "Finish", "DESC": "Finish", ";": "Finish"},
            "LIMIT": {"value": "Finish"},
            "NATURAL JOIN": {"JOIN": "JOIN"},
            "ORDER BY": {"BY": "ORDER BY"}
        }
        self.current_state = "Start"

    def validate(self, tokens):
        for token in tokens:
            if self.current_state == "Finish":
                break

            if token in self.transitions.get(self.current_state, {}):
                self.current_state = self.transitions[self.current_state][token]
            elif re.match(r"[=><!*]+", token):
                self.current_state = "operator"
            elif re.match(r"[0-9]+", token):
                self.current_state = "value"
            elif self.current_state == "LIMIT" and re.match(r"^\d+$", token):
                self.current_state = "Finish"
            elif self.current_state == "LIMIT":
                raise ValueError(f"Invalid value '{token}' for LIMIT; must be an integer")
            elif re.match(r"[A-Za-z_][A-Za-z0-9_]*", token):
                if token in self.SQL_KEYWORDS:
                    if self.current_state in ["attribute", "value"]:
                        self.current_state = "attribute"
                    else:
                        raise ValueError(f"Unexpected keyword '{token}' in state '{self.current_state}'")
                if self.current_state == "FROM":
                    self.current_state = "relation"
                elif self.current_state in ["WHERE", "ON condition"]:
                    self.current_state = "condition"
                elif self.current_state == "SET":
                    self.current_state = "attribute=value"
                elif self.current_state == "ORDER BY":
                    self.current_state = "ORDER direction"
                else:
                    self.current_state = "attribute"
            elif token == ";":
                self.current_state = "Finish"
            else:
                raise ValueError(f"Invalid token '{token}' in state '{self.current_state}'")

        if self.current_state != "Finish":
            raise ValueError("Query did not reach a valid end state")
        return True

# def tokenize(query):
#     return re.findall(r"[A-Za-z_][A-Za-z0-9_]*|[=><!]+|[0-9]+|,|;|\*", query)

# # Uji coba query
# query = "UPDATE table_name WHERE column_1 = 5 SET column_2 = column_3;"
# tokens = tokenize(query)

# # Validasi query
# validator = SQLValidator()
# try:
#     validator.validate(tokens)
#     print("Query valid!")
# except ValueError as e:
#     print(f"Query invalid: {e}")
