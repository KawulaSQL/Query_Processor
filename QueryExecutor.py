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

import re

class QueryExecutor:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.storage_manager = StorageManager(base_path)
        self.is_transacting = False

    def execute_select(self, query: str):
        optimizer = QueryOptimizer(query)
        parsed_result = optimizer.parse()
        print("Parsed Query Tree:")
        print_tree(parsed_result.query_tree)

        optimized_tree = optimizer.optimize(parsed_result.query_tree)
        print("\nOptimized Query Tree:")
        print_tree(optimized_tree)

        self.execute_query(optimized_tree)

    def execute_insert(self, statement: str):
        """
        Parse the INSERT INTO table_name VALUES (...) statement and directly execute it through the Storage Manager.

        :param statement: The SQL INSERT query string.
        """
        try:
            if not statement.startswith("INSERT INTO"):
                print("Error: Invalid INSERT statement.")
                return
            
            parts = statement.split("INSERT INTO")[1].split("VALUES")
            table_name = parts[0].strip()

            # As Transaction is beginning, operation is starting to be created
            if self.is_transacting:
                la_table = table_name if isinstance(table_name, list[str]) else list(table_name)
                operations = self.convert_to_operation(la_table, self.transact_id)
                for operation in operations:
                    self.operations.append(operation)

            values_str = parts[1].strip()[1:-1]

            rows = values_str.split('),')
            values_list = []
            for row in rows:
                row_values = row.strip().strip('()').split(',')
                values_list.append([v.strip() for v in row_values])

            if not values_list:
                print(f"Error: No valid data to insert into '{table_name}'.")
                return

            self.storage_manager.insert_into_table(table_name, values_list)
            print(f"Data inserted into '{table_name}' successfully.")
        
        except Exception as e:
            print(f"Error executing INSERT query: {e}")

    def execute_create(self, query: str):
        """
        Directly execute a CREATE query through the Storage Manager.

        :param query: The SQL CREATE query string.
        """
        try:
            if query.startswith("CREATE TABLE"):
                table_name = query.split()[2]
                
                columns_section = query.split("(")[1].split(")")[0]
                columns = columns_section.split(",")
                
                attributes = []
                for col in columns:
                    col_name, col_type = col.strip().split(" ")
                    attributes.append(Attribute(col_name, col_type, 10))
                
                schema = Schema(attributes)
                
                self.storage_manager.create_table(table_name, schema)
                print(f"Successfully created table: {table_name}")
                
                tables = self.storage_manager.list_tables()
                if table_name in tables:
                    print(f"Table {table_name} successfully created and found in the storage.")
                else:
                    print(f"Error: Table {table_name} was not found after creation.")
            
            else:
                print("Invalid CREATE query format.")
        
        except Exception as e:
            print(f"Error executing CREATE query: {e}")

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
        if query_tree.type == 'limit':
            limit_value = int(query_tree.condition)
            if query_tree.child and query_tree.child[0].type == 'table':
                table_name = query_tree.child[0].val
                table_data = self.storage_manager.get_table_data(table_name)
                if table_data:
                    schema = self.storage_manager.get_table_schema(table_name)
                    table_data = table_data[:limit_value]
                    self.display_table_data(table_data, schema)
                else:
                    print(f"No data found in table '{table_name}'.")
            else:
                print("Error: No valid table node found under limit node.")
        
        elif query_tree.type == 'project':
            if query_tree.child and query_tree.child[0].type == 'table':
                table_name = query_tree.child[0].val
            elif query_tree.child and query_tree.child[0].type == 'limit':
                limit_value = int(query_tree.child[0].condition)
                if query_tree.child[0].child and query_tree.child[0].child[0].type == 'table':
                    table_name = query_tree.child[0].child[0].val
                    table_data = self.storage_manager.get_table_data(table_name)
                    if table_data:
                        schema = self.storage_manager.get_table_schema(table_name)
                        table_data = table_data[:limit_value]
                        self.display_table_data(table_data, schema)
                    else:
                        print(f"No data found in table '{table_name}'.")
                else:
                    print("Error: No valid table node found under limit node.")
                    return
            else:
                print("Error: No valid table node found under project node.")
                return

            # Parse columns and aliases
            columns = query_tree.condition.split(',')
            column_mapping = {}
            for col in columns:
                match = re.match(r'\s*(\S+)\s+AS\s+(\S+)\s*', col.strip(), re.IGNORECASE)
                if match:
                    original = match.group(1).strip() 
                    alias = match.group(2).strip() 
                    column_mapping[original] = alias
                else:
                    column_mapping[col.strip()] = col.strip()

            # Get table data and schema
            table_data = self.storage_manager.get_table_data(table_name)
            if table_data:
                schema = self.storage_manager.get_table_schema(table_name)
                
                if query_tree.child and query_tree.child[0].type == 'limit':
                    limit_value = int(query_tree.condition)
                    table_data = table_data[:limit_value]

                self.display_projected_data_with_alias(
                    table_data, schema, column_mapping
                )
            else:
                print(f"No data found in table '{table_name}'.")

        elif query_tree.type == 'table':
            table_name = query_tree.val
            table_data = self.storage_manager.get_table_data(table_name)
            if table_data:
                schema = self.storage_manager.get_table_schema(table_name)
                self.display_table_data(table_data, schema)
            else:
                print(f"No data found in table '{table_name}'.")

    def display_projected_data(self, table_data, schema, columns):
        """
        Display the table data for projected columns (SELECT column1, column2).
        """
        column_names = [attr[0] for attr in schema.get_metadata()]

        # Get the indices of the requested columns
        column_indices = [column_names.index(col.strip()) for col in columns]

        # Filter data to show only the selected columns
        projected_data = [[row[idx] for idx in column_indices] for row in table_data]

        column_widths = [len(col) for col in columns]
        for row in projected_data:
            column_widths = [max(width, len(str(value))) for width, value in zip(column_widths, row)]

        row_format = " | ".join(f"{{:<{width}}}" for width in column_widths)
        separator = "-+-".join("-" * width for width in column_widths)

        print(row_format.format(*columns))
        print(separator)
        for row in projected_data:
            print(row_format.format(*row))

    def display_projected_data_with_alias(self, table_data, schema, column_mapping):
        """
        Display the table data for projected columns (SELECT column1 AS alias1, ...).
        """
        column_names = [attr[0] for attr in schema.get_metadata()]
        column_indices = []
        # Map original columns to indices
        for original_column in column_mapping.keys():
            if original_column in column_names:
                column_indices.append(column_names.index(original_column))
            else:
                print(f"Error: Column '{original_column}' is not in table schema.")
                return

        # Filter data to show only the selected columns
        projected_data = [
            [row[idx] for idx in column_indices] for row in table_data
        ]

        # Use aliases as headers
        aliases = list(column_mapping.values())
        column_widths = [len(alias) for alias in aliases]

        # Calculate column widths based on data
        for row in projected_data:
            column_widths = [
                max(width, len(str(value)))
                for width, value in zip(column_widths, row)
            ]
  
        # Print table with aliases as headers
        row_format = " | ".join(f"{{:<{width}}}" for width in column_widths)
        separator = "-+-".join("-" * width for width in column_widths)

        print(row_format.format(*aliases))
        print(separator)
        for row in projected_data:
            print(row_format.format(*row))

    def display_table_data(self, table_data, schema):
        """
        Display the full table data (SELECT * FROM table).
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