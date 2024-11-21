import re

class SQLValidator:
    def __init__(self):
        # Daftar kata kunci SQL untuk validasi
        self.SQL_KEYWORDS = {"SELECT", "FROM", "WHERE", "JOIN", "AS", "NATURAL", "ON", "UPDATE", "COMMIT", "SET", "ORDER", "BY", "LIMIT"}

        # State transitions
        self.transitions = {
            "Start": {"SELECT": "SELECT", "UPDATE": "UPDATE", "BEGIN": "BEGIN TRANSACTION"},
            "SELECT": {"*": "*", "attribute": "attribute"},
            "attribute": {"AS": "AS alias", ",": ",", "FROM": "FROM", "WHERE": "WHERE", "ORDER": "ORDER BY", "LIMIT": "LIMIT", ";": "Finish"},
            "AS alias": {"attribute": "attribute", "FROM": "FROM"},
            "FROM": {"relation": "relation"},
            "relation": {"AS": "AS relation", "WHERE": "WHERE", "NATURAL": "NATURAL JOIN", "JOIN": "JOIN", ";": "Finish", "ORDER": "ORDER BY", "LIMIT": "LIMIT", "SET":"SET", "ON":"relation ON"},
            "JOIN": {"relation": "relation"},
            "relation ON": {"ON": "ON condition", "relation": "relation"},
            "ON condition": {"attribute": "condition"},
            "condition": {"operator": "condition value"},
            "condition value": {"value": "WHERE", ";": "Finish"},
            "WHERE": {"attribute": "condition", "ORDER": "ORDER BY", ";": "Finish"},
            "operator": {"attribute": "condition", "value": "value"},
            "value": {";": "Finish", "ORDER": "ORDER BY", "LIMIT": "LIMIT", "WHERE": "WHERE", "*": "*"},
            "value-string": {";": "Finish", "ORDER": "ORDER BY", "LIMIT": "LIMIT", "WHERE": "WHERE", "*": "*"},
            "UPDATE": {"relation": "SET"},
            "SET": {"attribute": "attribute"},
            "attribute=value": {"WHERE": "WHERE"},
            "BEGIN TRANSACTION": {";": "Finish", "COMMIT": "COMMIT", "TRANSACTION": "TRANSACTION"},
            "TRANSACTION": {";": "Finish"},
            "COMMIT": {";": "Finish"},
            "ORDER BY": {"attribute": "ORDER direction"},
            "ORDER direction": {"ASC": "Finish", "DESC": "Finish", ";": "Finish"},
            "LIMIT": {"value": "Finish"},
            "NATURAL JOIN": {"JOIN": "JOIN"},
            "ORDER BY": {"BY": "ORDER BY"},
            ",": {"attribute": "attribute"},
            "*": {"FROM": "FROM", "attribute": "attribute"},
            ".": {"attribute": "attribute"},
            "relation of AS": {"relation": "relation", "WHERE": "WHERE"},
        }
        self.current_state = "Start"

    def validate(self, tokens):
        for token in tokens:
            if self.current_state == "Finish":
                break

            # Memproses token untuk state tertentu
            if token in self.transitions.get(self.current_state, {}):
                self.current_state = self.transitions[self.current_state][token]
            
            # Menangani token yang tidak valid di state tertentu
            elif token not in self.transitions.get(self.current_state, {}):
                # Memproses token khusus
                if re.match(r"[=><!*]+", token):
                    if token == "==" or token == "!=":
                        raise ValueError(f"Invalid operator '{token}' in state '{self.current_state}'")
                    elif self.current_state == "SET" or self.current_state == "attribute=value":
                        self.current_state = "attribute=value"
                    elif "operator" in self.transitions.get(self.current_state, {}):
                        self.current_state = "operator"
                    elif self.current_state == "attribute":
                        self.current_state = "operator"
                    else:
                        raise ValueError(f"Invalid token '{token}' in state '{self.current_state}'")
                
                # Token angka hanya valid di dalam konteks value
                elif re.match(r"[0-9]+", token):
                    if self.current_state == "attribute=value":
                        self.current_state = "attribute=value"
                    if "value" in self.transitions.get(self.current_state, {}):
                        self.current_state = "value"
                    else:
                        raise ValueError(f"Invalid token '{token}' in state '{self.current_state}'")
                    
                # Token string hanya valid di dalam konteks value
                elif re.match(r"^'.*'$|^\".*\"$", token):
                    if self.current_state == "attribute=value":
                        self.current_state = "attribute=value"
                    if "value" in self.transitions.get(self.current_state, {}):
                        self.current_state = "value-string"
                    else:
                        raise ValueError(f"Invalid token '{token}' in state '{self.current_state}'")
                    
                # Token untuk handle bilangan negatif
                elif re.match(r"[-+]?[0-9]+(?:\.[0-9]+)?", token):
                    if self.current_state == "LIMIT":
                        raise ValueError(f"Invalid token '{token}' in state '{self.current_state}'")


                # Token kata valid untuk keyword atau attribute
                elif re.match(r"[A-Za-z_][A-Za-z0-9_]*", token):
                    if token in self.SQL_KEYWORDS:
                        if self.current_state == "attribute":
                            self.current_state = "attribute"

                        elif self.current_state == "FROM":
                            self.current_state = "relation"

                        elif self.current_state == "relation":
                            if token not in self.transitions["relation"]:
                                raise ValueError(f"Invalid keyword '{token}' in state '{self.current_state}'")
                            self.current_state = self.transitions["relation"][token]

                        elif self.current_state in ["WHERE", "ON condition"]:
                            self.current_state = "condition"

                        elif self.current_state == "SET":
                            self.current_state = "attribute=value"

                        elif self.current_state == "ORDER BY":
                            self.current_state = "ORDER direction"
                            
                        else:
                            raise ValueError(f"Unexpected keyword '{token}' in state '{self.current_state}'")
                        
                    elif self.current_state == "attribute":
                        if token in self.transitions.get(self.current_state, {}):
                            self.current_state = self.transitions[self.current_state][token]
                        else:
                            raise ValueError(f"Invalid keyword '{token}' in state '{self.current_state}'")
                        
                    elif self.current_state == ",":
                        self.current_state = "attribute"

                    elif self.current_state == "WHERE":
                        self.current_state = "condition"

                    elif self.current_state == "UPDATE":
                        self.current_state = "relation"
                    
                    elif self.current_state == "SET":
                        self.current_state = "attribute"

                    elif self.current_state == "attribute=value":
                        self.current_state = "WHERE"

                    elif self.current_state == "JOIN":
                        self.current_state = "relation"

                    elif self.current_state == "*":
                        if token == "FROM":
                            self.current_state = "FROM"
                        else:
                            self.current_state = "attribute"
                        
                    elif self.current_state == "ORDER BY":
                        self.current_state = "ORDER direction"

                    elif self.current_state == "AS relation":
                        self.current_state = "relation of AS"

                    elif self.current_state == "relation of AS":
                        self.current_state = "relation"

                    elif self.current_state == "AS alias":
                        self.current_state = "attribute"
                    
                    elif token == ",":
                        self.current_state = "attribute"

                    elif self.current_state == ".":
                        self.current_state = "attribute"

                    elif self.current_state == "condition":
                        self.current_state = "attribute"

                    elif self.current_state == "SELECT":
                        self.current_state = "attribute"

                    elif self.current_state == "operator":
                        self.current_state = "condition"

                    elif self.current_state == "FROM":
                        self.current_state = "relation"

                    elif self.current_state == "LIMIT":
                        self.current_state = "attribute=value"

                    elif self.current_state == "relation ON":
                        self.current_state = "."

                    elif self.current_state == "attribute=value":
                        if token not in self.transitions.get(self.current_state, {}):
                            raise ValueError(f"Invalid keyword '{token}' in state '{self.current_state}'")
                        
                    else:
                        raise ValueError(f"Invalid keyword '{token}' in state '{self.current_state}'")

        # Validasi state akhir
        if self.current_state != "Finish":
            raise ValueError("Query did not reach a valid end state")
        return True


def tokenize(query):
    return re.findall(r"'.*?'|\".*?\"|[A-Za-z_][A-Za-z0-9_]*|[0-9]+(?:\.[0-9]+)?|[-+]?[0-9]+(?:\.[0-9]+)?|[=><!]+|,|;|\*|\.", query)




# Uji coba query
query = "SELECT * FROM table1 LIMIT -5;"
tokens = tokenize(query)
print(tokens)

# Validasi query
validator = SQLValidator()
try:
    validator.validate(tokens)
    print("Query valid!")
except ValueError as e:
    print(f"Query invalid: {e}")
