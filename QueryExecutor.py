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
from Concurrency_Control_Manager.ConcurrencyControlManager import ConcurrencyControlManager
from Concurrency_Control_Manager.models import Operation
from Concurrency_Control_Manager.models import CCManagerEnums

class QueryExecutor:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.storage_manager = StorageManager(base_path)
        self.conccurency_control_manager = ConcurrencyControlManager()
        self.is_transacting = False
        self.operations = []
        self.transact_id = 1

    def execute_select(self, query: str):
        optimizer = QueryOptimizer(query)
        parsed_result = optimizer.parse()
        print("Parsed Query Tree:")
        print_tree(parsed_result.query_tree)

        table_name = self.get_table_names(parsed_result.query_tree)
        operations = self.convert_to_operation_select(table_name, self.transact_id)
        # response = self.check_for_response(operations)

        # if response.responseType == "Abort":
        #   return "Execution not allowed"

        if self.is_transacting:
            for operation in operations:
                self.operations.append(operation)

        optimized_tree = optimizer.optimize(parsed_result.query_tree)
        print("\nOptimized Query Tree:")
        print_tree(optimized_tree)

        self.execute_query(optimized_tree)

    def execute_insert(self, query: str):
        table_name, values = self.parse_insert(query)

        operations = self.convert_to_operation_insert(table_name, self.transact_id)

        # response = self.check_for_response(operations)

        # if response.responseType == "Abort":
        #   return "Execution not allowed"

        if self.is_transacting:
            for operation in operations:
                self.operations.append(operation)

        self.storage_manager.insert_into_table(table_name, values)
        print(f"Data inserted into '{table_name}' successfully.")

    def execute_create(self, query: str):
        table_name, schema = self.parse_create(query)
        self.storage_manager.create_table(table_name, schema)
        print(f"Successfully created table: {table_name}")

    def begin_transaction(self):
        self.is_transacting = True
        print("Transaction started.")
        # Implement the logic to begin transaction

    def commit(self):
        print("Transaction committed.")
        begins = self.conccurency_control_manager.begin_transaction(self.operations)
        # implement the concurrency control thingy
        # Implement the logic to commit transaction

    def check_for_response(self, operations: list):
        begins = self.conccurency_control_manager.begin_transaction(operations)
        # Implement the concurrency control thingy
        # response = self.conccurency_control_manager.send_response_to_processor(response)
        # return response

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

    def get_table_names(self, query_tree: QueryTree) -> list[str]:
        """
        Get all table names queried by user
        """
        liste_table = []
        while query_tree is not None:
            if query_tree.type == 'table':
                liste_table.append(query_tree.val)
            query_tree = query_tree.child[0]
        return liste_table

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
    
    def convert_to_operation_select(self, table_name: list[str], transact_id: int):
        operations = []
        for table in table_name:
            operations.append(Operation(CCManagerEnums.OperationType.R, f"tx{transact_id}", table))
        return operations
    
    def convert_to_operation_insert(self, table_name: list[str], transact_id: int):
        operations = []
        for table in table_name:
            operations.append(Operation(CCManagerEnums.OperationType.W, f"tx{transact_id}", table))
        return operations