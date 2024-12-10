import sys
sys.path.append('./Query_Optimizer')
sys.path.append('./Storage_Manager')
sys.path.append('./Concurrency_Control_Manager')

from Query_Optimizer.QueryOptimizer import QueryOptimizer
from Query_Optimizer.model.models import QueryTree
from Storage_Manager.StorageManager import StorageManager
from Storage_Manager.lib.Schema import Schema
from Storage_Manager.lib.Attribute import Attribute
# from Concurrency_Control_Manager.ConcurrencyControlManager import ConcurrencyControlManager
# from Concurrency_Control_Manager.models import Operation
# from Concurrency_Control_Manager.models import CCManagerEnums
from bang.bangs import ExecutionResult, Rows
from datetime import datetime
from typing import Tuple
from utils.query import get_query_type

import re

class QueryExecutor:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.storage_manager = StorageManager(base_path)
        # self.conccurency_control_manager = ConcurrencyControlManager()
        self.is_transacting = False
        self.operations = []
        self.transact_id = 0

    def execute_select(self, query: str) -> ExecutionResult:
        try:
            optimizer = QueryOptimizer(query)
            parsed_result = optimizer.parse()
            type = get_query_type(query)

            # table_name = self.get_table_names(parsed_result.query_tree)
            # operations = self.convert_to_operation_select(table_name, self.transact_id)

            # if self.is_transacting:
            #     for operation in operations:
            #         self.operations.append(operation)

            optimized_tree = optimizer.optimize(parsed_result.query_tree)

            result_data, schema, columns = self.execute_query(optimized_tree)

            timestamp = datetime.now()
            previous_data = Rows(data=[], rows_count=0, schema=[], columns={})
            new_data = Rows(data=result_data, rows_count=len(result_data), schema=schema, columns=columns)

            return ExecutionResult(
                transaction_id=self.transact_id,
                timestamp=timestamp,
                type=type,
                status="success",
                query=query,
                previous_data=previous_data,
                new_data=new_data
            )
        
        except Exception as e:
            return ExecutionResult(
                transaction_id=self.transact_id,
                timestamp=datetime.now(),
                type=type,
                status="error",
                query=query,
                previous_data=Rows(data=[], rows_count=0, columns=[]),
                new_data=Rows(data=[], rows_count=0, columns=[])
            )

    def execute_insert(self, query: str) -> ExecutionResult:
        try:
            if not query.startswith("INSERT INTO"):
                raise ValueError("Error: Invalid INSERT query.")
            
            parts = query.split("INSERT INTO")[1].split("VALUES")
            table_name = parts[0].strip()
            values_str = parts[1].strip()[1:-1]
            
            rows = values_str.split('),')
            values_list = []
            for row in rows:
                row_values = row.strip().strip('()').split(',')
                values_list.append([v.strip() for v in row_values])
            
            if not values_list:
                raise ValueError(f"Error: No valid data to insert into '{table_name}'.")
            
            self.storage_manager.insert_into_table(table_name, values_list)
            schema = self.storage_manager.get_table_schema(table_name)
            columns = [attr[0] for attr in schema.get_metadata()]
            
            timestamp = datetime.now()
            new_data = Rows(
                data=values_list,
                rows_count=len(values_list),
                schema=schema,
                columns=columns
            )
            
            return ExecutionResult(
                transaction_id=self.transact_id,
                timestamp=timestamp,
                type="INSERT",
                status="success",
                query=query,
                previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
                new_data=new_data
            )
        
        except Exception as e:
            return ExecutionResult(
                transaction_id=self.transact_id,
                timestamp=datetime.now(),
                type="INSERT",
                status="error",
                query=query,
                previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
                new_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
            )


    def execute_create(self, query: str):
        """
        Directly execute a CREATE query through the Storage Manager.

        :param query: The SQL CREATE query string.
        """
        schema_match = re.match(r"CREATE TABLE (\w+)\s*\((.+)\)", query, re.IGNORECASE)
        
        if not schema_match:
            print("Error: Invalid CREATE TABLE statement.")
            return

        table_name = schema_match.group(1).strip()
        schema_str = schema_match.group(2).strip()

        attributes = []
        for attribute_str in schema_str.split(','):
            attribute_str = attribute_str.strip()
            match = re.match(r"(\w+)\s+(\w+)(?:\((\d+)\))?", attribute_str)
            if match:
                name = match.group(1)
                dtype = match.group(2).lower()
                size = int(match.group(3)) if match.group(3) else None
                attributes.append(Attribute(name, dtype, size))
            else:
                print(f"Error: Invalid attribute definition '{attribute_str}'")
                return

        try:
            self.storage_manager.create_table(table_name, Schema(attributes))
            print(f"{table_name} successfully created")
        except ValueError as e :
            print(e)

    def begin_transaction(self):
        self.is_transacting = True
        print("Transaction started.")
        # Implement the logic to begin transaction

    def commit(self):
        print("Transaction committed.")
        self.transact_id += 1
        begins = self.conccurency_control_manager.begin_transaction(self.operations)
        # implement the concurrency control thingy
        # Implement the logic to commit transaction

    def check_for_response(self, operations: list):
        self.transact_id += 1
        begins = self.conccurency_control_manager.begin_transaction(operations)
        # Implement the concurrency control thingy
        # response = self.conccurency_control_manager.send_response_to_processor(response)
        # return response

    def execute_query(self, query_tree: QueryTree) -> Tuple[list, list, map]:
        """
        Execute the query based on the optimized query tree and return the result as a list
        along with the schema.
        """
        try:
            result_data = []
            schema = []
            columns = {}

            if query_tree.type == 'limit':
                limit_value = int(query_tree.condition)
                if query_tree.child and query_tree.child[0].type == 'table':
                    table_name = query_tree.child[0].val
                    table_data = self.storage_manager.get_table_data(table_name)
                    if table_data:
                        schema = self.storage_manager.get_table_schema(table_name)
                        result_data = table_data[:limit_value]
                        columns = [attr[0] for attr in schema.get_metadata()]
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
                            result_data = table_data[:limit_value]
                        else:
                            print(f"No data found in table '{table_name}'.")
                    else:
                        print("Error: No valid table node found under limit node.")
                        return [], [], {}

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

                columns = column_mapping
                # Get table data and schema
                table_data = self.storage_manager.get_table_data(table_name)
                if table_data:
                    schema = self.storage_manager.get_table_schema(table_name)
                    if query_tree.child and query_tree.child[0].type == 'limit':
                        limit_value = int(query_tree.child[0].condition)
                        result_data = table_data[:limit_value]
                    else:
                        result_data = table_data

                    # self.display_projected_data_with_alias(result_data, schema, column_mapping)
                else:
                    print(f"No data found in table '{table_name}'.")

            elif query_tree.type == 'table':
                table_name = query_tree.val
                table_data = self.storage_manager.get_table_data(table_name)
                if table_data:
                    schema = self.storage_manager.get_table_schema(table_name)
                    columns = [attr[0] for attr in schema.get_metadata()]
                    result_data = table_data
                    # self.display_table_data(result_data, schema)
                else:
                    print(f"No data found in table '{table_name}'.")

            return result_data, schema, columns

        except Exception as e:
            print(f"Error executing query: {e}")
            return [], [], {}

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
    
    # def convert_to_operation_select(self, table_name: list[str], transact_id: int):
    #     operations = []
    #     for table in table_name:
    #         operations.append(Operation(CCManagerEnums.OperationType.R, f"tx{transact_id}", table))
    #     return operations
    
    # def convert_to_operation_insert(self, table_name: list[str], transact_id: int):
    #     operations = []
    #     for table in table_name:
    #         operations.append(Operation(CCManagerEnums.OperationType.W, f"tx{transact_id}", table))
    #     return operations