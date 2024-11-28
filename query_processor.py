import sys
sys.path.append('./Query_Optimizer')
sys.path.append('./Storage_Manager')

from Query_Optimizer.QueryOptimizer import QueryOptimizer
from Query_Optimizer.model.models import QueryTree
from Storage_Manager.StorageManager import StorageManager
from Storage_Manager.test_driver import TestDriver

class QueryProcessor:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.storage_manager = StorageManager(base_path)
    
    def process_query(self, query: str):
        """
        Process the query using the Query Optimizer and Storage Manager.
        Display the query results to the user after optimization.

        :param query: SQL query as a string.
        """
        try:
            # Step 1: Parse and optimize the query
            optimizer = QueryOptimizer(query)
            parsed_result = optimizer.parse()
            print("Parsed Query Tree:")
            self.print_tree(parsed_result.query_tree)

            optimized_tree = optimizer.optimize(parsed_result.query_tree)
            print("\nOptimized Query Tree:")
            self.print_tree(optimized_tree)

            # Step 2: Execute the query with Storage Manager based on the optimized tree
            self.execute_query(optimized_tree)

        except Exception as e:
            print(f"Error processing query: {e}")

    def execute_query(self, query_tree: QueryTree):
        """
        Execute the query based on the tree received from the QueryOptimizer.
        Adapt it with Storage Manager to display the query result.

        :param query_tree: The optimized QueryTree.
        """
        if query_tree is not None:
            if query_tree.type == 'table':
                table_name = query_tree.val
                print(f"Executing SELECT on table: {table_name}")
                table_data = self.storage_manager.get_table_data(table_name)
                
                if table_data:
                    # Displaying the result in a formatted way
                    schema = self.storage_manager.get_table_schema(table_name)
                    column_names = [attr[0] for attr in schema.get_metadata()]
                    self.display_table_data(table_data, column_names)
                else:
                    print(f"No data found in table '{table_name}'.")

            else:
                print("Unsupported query type:", query_tree.type)

    def display_table_data(self, table_data, column_names):
        """
        Display the table data in a formatted way.

        :param table_data: The data retrieved from the table.
        :param column_names: The column names of the table.
        """
        column_widths = [len(name) for name in column_names]
        for row in table_data:
            column_widths = [max(width, len(str(value))) for width, value in zip(column_widths, row)]

        row_format = " | ".join(f"{{:<{width}}}" for width in column_widths)
        separator = "-+-".join("-" * width for width in column_widths)

        print(row_format.format(*column_names))
        print(separator)
        for row in table_data:
            print(row_format.format(*row))

    def print_tree(self, tree: QueryTree, level=0):
        """
        Recursively print the QueryTree.

        :param tree: The QueryTree to be printed.
        :param level: The depth level of the tree.
        """
        if tree is not None:
            print("    " * level + f"Type: {tree.type}, Value: {tree.val}, Condition: {tree.condition}")
            for child in tree.child:
                self.print_tree(child, level + 1)


# Main program
if __name__ == "__main__":
    base_path = "./Storage_Manager/storage"
    query_processor = QueryProcessor(base_path)

    query = "SELECT * FROM users;"
    print(f"Original Query: {query}\n")
    
    query_processor.process_query(query)