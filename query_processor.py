import sys
sys.path.append('./Query_Optimizer')

from Query_Optimizer.QueryOptimizer import QueryOptimizer
from Query_Optimizer.model.models import QueryTree

def main():
    query = "SELECT * FROM users WHERE age > 25 AND name LIKE '%John%' ORDER BY age DESC LIMIT 10"

    print(f"Original Query: {query}\n")

    try:
        optimizer = QueryOptimizer(query)

        parsed_result = optimizer.parse()
        print("Parsed Query Tree:")
        print_tree(parsed_result.query_tree)

        optimized_tree = optimizer.optimize(parsed_result.query_tree)
        print("\nOptimized Query Tree:")
        print_tree(optimized_tree)

    except Exception as e:
        print(f"Error processing query: {e}")

def print_tree(tree: QueryTree, level=0):
    """
    Fungsi untuk mencetak QueryTree secara rekursif.
    """
    if tree is not None:
        print("    " * level + f"Type: {tree.type}, Value: {tree.val}, Condition: {tree.condition}")
        for child in tree.child:
            print_tree(child, level + 1)

if __name__ == "__main__":
    main()