def get_query_type(query: str):
    """
    Determine the type of SQL query (e.g., SELECT, INSERT, CREATE).
    """
    query = query.strip().upper()

    if query.startswith("SELECT"):
        return "SELECT"
    elif query.startswith("INSERT"):
        return "INSERT"
    elif query.startswith("CREATE"):
        return "CREATE"
    elif query.startswith("UPDATE"):
        return "UPDATE"
    elif query.startswith("BEGIN TRANSACTION"):
        return "BEGIN TRANSACTION"
    elif query.startswith("COMMIT"):
        return "COMMIT"
    else:
        return "UNKNOWN"

def print_tree(tree, level=0):
    """
    Recursively print the QueryTree.
    """
    if tree is not None:
        print("    " * level + f"Type: {tree.type}, Value: {tree.val}, Condition: {tree.condition}")
        for child in tree.child:
            print_tree(child, level + 1)