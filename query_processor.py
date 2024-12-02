import sys
sys.path.append('./Query_Optimizer')
sys.path.append('./Storage_Manager')

from Query_Optimizer.QueryOptimizer import QueryOptimizer
from Query_Optimizer.model.models import QueryTree
from Concurrency_Control_Manager.ConcurrencyControlManager import ConcurrencyControlManager
from Concurrency_Control_Manager.models import Operation
from Concurrency_Control_Manager.models import CCManagerEnums
from Concurrency_Control_Manager.models import Resource
from Storage_Manager.StorageManager import StorageManager
from Storage_Manager.lib.Schema import Schema
from Storage_Manager.lib.Attribute import Attribute
import threading
import time

class QueryProcessor:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.storage_manager = StorageManager(base_path)
        self.concurrency_control_manager = ConcurrencyControlManager()
        self.operations = []
        self.is_transacting = False
        self.transact_id = 1
    
    def process_query(self, query: str):
        """
        Process the query using the Query Optimizer for SELECT queries
        and directly execute INSERT and CREATE queries with Storage Manager.

        :param query: SQL query as a string.
        """
        try:
            query_type = self.get_query_type(query)
            
            success = True
            if query_type == "SELECT":
                optimizer = QueryOptimizer(query)
                parsed_result = optimizer.parse()
                print("Parsed Query Tree:")
                self.print_tree(parsed_result.query_tree)

                optimized_tree = optimizer.optimize(parsed_result.query_tree)
                print("\nOptimized Query Tree:")
                self.print_tree(optimized_tree)

                self.execute_query(optimized_tree)

            elif query_type == "INSERT":
                self.execute_insert(query)
                
            elif query_type == "CREATE":
                self.execute_create(query)

            elif query_type == "BEGIN TRANSACTION":
                self.begin_transaction()

            elif query_type == "COMMIT":
                self.commit()
            
            else:
                print(f"Unsupported query type: {query_type}")

                success = False

            return success

        except Exception as e:
            print(f"Error processing query: {e}")

            return False

    def get_query_type(self, query: str):
        """
        Determines the type of SQL query (e.g., SELECT, INSERT, CREATE).

        :param query: The SQL query string.
        :return: A string representing the query type.
        """
        query = query.strip().upper()
        
        if query.startswith("SELECT"):
            return "SELECT"
        elif query.startswith("INSERT"):
            return "INSERT"
        elif query.startswith("CREATE"):
            return "CREATE"
        elif query.startswith("BEGIN TRANSACTION"):
            return "BEGIN TRANSACTION"
        elif query.startswith("COMMIT"):
            return "COMMIT"
        else:
            return "UNKNOWN"

    def execute_query(self, query_tree: QueryTree):
        """
        Execute the query based on the tree received from the QueryOptimizer.
        Adapt it with Storage Manager to display the query result.

        :param query_tree: The optimized QueryTree.
        """
        if query_tree is not None:
            if query_tree.type == 'table':
                table_name = query_tree.val

                # As Transaction is beginning, operation is starting to be created
                if self.is_transacting:
                    la_table = table_name if isinstance(table_name, list[str]) else list(table_name)
                    operations = self.convert_to_operation(la_table, self.transact_id)
                    for operation in operations:
                        self.operations.append(operation)

                print(f"Executing SELECT on table: {table_name}")
                table_data = self.storage_manager.get_table_data(table_name)
                
                if table_data:
                    schema = self.storage_manager.get_table_schema(table_name)
                    if isinstance(schema, list):
                        print(f"Schema metadata for table '{table_name}': {schema}")
                    else:
                        column_names = [attr[0] for attr in schema.get_metadata()]
                        self.display_table_data(table_data, column_names)
                else:
                    print(f"No data found in table '{table_name}'.")
            else:
                print("Unsupported query type:", query_tree.type)

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

    def begin_transaction(self):
        self.is_transacting = True

    def convert_to_operation(self, table_names: list[str], transact_id: int):
        """
        Converting query to an Operation Data
        """
        try:
            if self.is_transacting:
                operation_liste = []
                if query.startswith("SELECT"):
                    for table_name in table_names:
                        operation_liste.append(Operation(CCManagerEnums.OperationType.R, f"tx{transact_id}", table_name))
                    return operation_liste
                elif query.startswith("INSERT"):
                    for table_name in table_names:
                        operation_liste.append(Operation(CCManagerEnums.OperationType.W, f"tx{transact_id}", table_name))
                    return operation_liste
                # elif query.startswith("UPDATE"):        
        except Exception as e:
            print(f"Query error {e}")

    def commit(self):
        """
        Commiting all the operations that are being stored
        """
        try:
            transaction = self.concurrency_control_manager.begin_transaction(self.operations)
            thread = threading.Thread(target=self.concurrency_control_manager.run)
            thread.start()
            time.sleep(5)
            self.concurrency_control_manager.stop()
            thread.join()
            self.operations.clear()
            self.is_transacting = False
        except Exception as e:
            print(f"Error: {e}")

# Main program
if __name__ == "__main__":
    base_path = "./Storage_Manager/storage"
    query_processor = QueryProcessor(base_path)

    query = input("Please enter your SQL query: ")
    print(f"Original Query: {query}\n")
    
    query_processor.process_query(query)