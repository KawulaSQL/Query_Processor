import sys
sys.path.append('./Query_Optimizer')
sys.path.append('./Storage_Manager')
sys.path.append('./Concurrency_Control_Manager')

from QueryUtils import print_tree
from Query_Optimizer.QueryOptimizer import QueryOptimizer
from Query_Optimizer.model.models import QueryTree
from Storage_Manager.StorageManager import StorageManager
from Storage_Manager.lib.Schema import Schema
from Storage_Manager.lib.Attribute import Attribute

class QueryExecutor:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.storage_manager = StorageManager(base_path)

    def execute_select(self, query: str):
        optimizer = QueryOptimizer(query)
        parsed_result = optimizer.parse()
        print("Parsed Query Tree:")
        print_tree(parsed_result.query_tree)

        optimized_tree = optimizer.optimize(parsed_result.query_tree)
        print("\nOptimized Query Tree:")
        print_tree(optimized_tree)

        self.execute_query(optimized_tree)

    def execute_insert(self, query: str):
        table_name, values = self.parse_insert(query)
        self.storage_manager.insert_into_table(table_name, values)
        print(f"Data inserted into '{table_name}' successfully.")

    def execute_create(self, query: str):
        table_name, schema = self.parse_create(query)
        self.storage_manager.create_table(table_name, schema)
        print(f"Successfully created table: {table_name}")

    def begin_transaction(self):
        print("Transaction started.")
        # Implement the logic to begin transaction

    def commit(self):
        print("Transaction committed.")
        # Implement the logic to commit transaction

    def execute_query(self, query_tree: QueryTree):
        """
        Execute the query based on the optimized query tree.
        """
        if query_tree.type == 'table':
            table_name = query_tree.val
            table_data = self.storage_manager.get_table_data(table_name)
            if table_data:
                schema = self.storage_manager.get_table_schema(table_name)
                print(f"Schema metadata for table '{table_name}': {schema}")
                self.display_table_data(table_data, schema)
            else:
                print(f"No data found in table '{table_name}'.")

    def display_table_data(self, table_data, schema):
        """
        Display the table data in a formatted way.
        """
        column_names = [attr[0] for attr in schema.get_metadata()]
        column_widths = [len(name) for name in column_names]
        for row in table_data:
            column_widths = [max(width, len(str(value))) for width, value in zip(column_widths, row)]

        row_format = " | ".join(f"{{:<{width}}}" for width in column_widths)
        separator = "-+-".join("-" * width for width in column_widths)

        print(row_format.format(*column_names))
        print(separator)
        for row in table_data:
            print(row_format.format(*row))

    def parse_insert(self, statement: str):
        """
        Parse an INSERT INTO statement.
        """
        parts = statement.split("INSERT INTO")[1].split("VALUES")
        table_name = parts[0].strip()
        values_str = parts[1].strip()[1:-1]
        rows = values_str.split('),')
        values_list = []
        for row in rows:
            row_values = row.strip().strip('()').split(',')
            values_list.append([v.strip() for v in row_values])

        return table_name, values_list

    def parse_create(self, query: str):
        """
        Parse a CREATE TABLE statement.
        """
        table_name = query.split()[2]
        columns_section = query.split("(")[1].split(")")[0]
        columns = columns_section.split(",")
        attributes = []
        for col in columns:
            col_name, col_type = col.strip().split(" ")
            attributes.append(col_name)  # Simplified for now

        return table_name, attributes