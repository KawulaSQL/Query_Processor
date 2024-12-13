import sys
sys.path.append('./Query_Optimizer')
sys.path.append('./Storage_Manager')
sys.path.append('./Concurrency_Control_Manager')

from Query_Optimizer.QueryOptimizer import QueryOptimizer
from Query_Optimizer.model.models import QueryTree
from Storage_Manager.StorageManager import StorageManager
from Storage_Manager.lib.Schema import Schema
from Storage_Manager.lib.Attribute import Attribute
from Storage_Manager.lib.Condition import Condition
from QueryConcurrencyController import QueryConcurrencyController
# from Concurrency_Control_Manager.ConcurrencyControlManager import ConcurrencyControlManager
# from Concurrency_Control_Manager.models import Operation
# from Concurrency_Control_Manager.models import CCManagerEnums
# from Concurrency_Control_Manager.models import Resource
from utils.models import ExecutionResult, Rows
from datetime import datetime
from typing import Tuple 
from utils.query import get_query_type, print_tree

import re

class QueryExecutor:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.storage_manager = StorageManager(base_path)
        # self.conccurency_control_manager = ConcurrencyControlManager()
        self.qcc = QueryConcurrencyController()
        self.is_transacting = False
        self.operations = []
        self.transact_id = 0

    def execute_select(self, query: str) -> ExecutionResult:
        try:
            optimizer = QueryOptimizer(query, self.storage_manager.get_stats())
            parsed_result = optimizer.parse()
            type = get_query_type(query)

            # table_name = self.get_table_names(parsed_result.query_tree)
            # operations = self.convert_to_operation_select(table_name, self.transact_id)

            # if self.is_transacting:
            #     for operation in operations:
            #         self.operations.append(operation)

            optimized_tree = optimizer.optimize(parsed_result)

            table_name = self.get_table_names(optimized_tree.query_tree) 
            print("table name:", table_name)
            print_tree(optimized_tree.query_tree)

            response = self.qcc.check_for_response_select(table_name)

            print(response)

            if (response != "OK"):
                print("Response is not OK")
                res = ExecutionResult(
                    transaction_id=self.qcc.transact_id,
                    timestamp=datetime.now(),
                    type=type,
                    status="error",
                    query=query,
                    previous_data=Rows(data=[], rows_count=0, columns=[]),
                    new_data=Rows(data=[], rows_count=0, columns=[])
                )
                for rollback_query in response:
                    if self.qcc.is_transacting and self.qcc.is_rollingback:
                        if (get_query_type(rollback_query) == 'SELECT'):
                            self.execute_select(rollback_query)
                        elif (get_query_type(rollback_query) == 'INSERT'):
                            self.execute_insert(rollback_query)
                        elif (get_query_type(rollback_query) == 'UPDATE'):
                            self.execute_update(rollback_query)
                        elif (get_query_type(rollback_query) == 'DELETE'):
                            self.execute_delete(rollback_query)
                self.qcc.is_rollingback = False
                return res

            result_data, schema, columns = self.execute_query(optimized_tree.query_tree)

            timestamp = datetime.now()
            previous_data = Rows(data=[], rows_count=0, schema=[], columns={})
            new_data = Rows(data=result_data, rows_count=len(result_data), schema=schema, columns=columns)

            res = ExecutionResult(
                transaction_id=self.qcc.transact_id,
                timestamp=timestamp,
                type=type,
                status="success",
                query=query,
                previous_data=previous_data,
                new_data=new_data
            )

            return res
        
        except Exception as e:
            return ExecutionResult(
                transaction_id=self.qcc.transact_id,
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
            
            response = self.qcc.check_for_response_insert(list(table_name))

            if (response != "OK"):
                res = ExecutionResult(
                    transaction_id=self.qcc.transact_id,
                    timestamp=datetime.now(),
                    type="UPDATE",
                    status="error",
                    query=query,
                    previous_data=Rows(data=[], rows_count=0, columns=[]),
                    new_data=Rows(data=[], rows_count=0, columns=[])
                )
                for rollback_query in response:
                    if self.qcc.is_transacting and self.qcc.is_rollingback:
                        if (get_query_type(rollback_query) == 'SELECT'):
                            self.execute_select(rollback_query)
                        elif (get_query_type(rollback_query) == 'INSERT'):
                            self.execute_insert(rollback_query)
                        elif (get_query_type(rollback_query) == 'UPDATE'):
                            self.execute_update(rollback_query)
                        elif (get_query_type(rollback_query) == 'DELETE'):
                            self.execute_delete(rollback_query)
                self.qcc.is_rollingback = False
                return res
            
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

            res = ExecutionResult(
                transaction_id=self.qcc.transact_id,
                timestamp=timestamp,
                type="INSERT",
                status="success",
                query=query,
                previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
                new_data=new_data
            )

            if (self.qcc.is_transacting):
                self.qcc.frm.write_log(res)
            
            return res
        
        except Exception as e:
            return ExecutionResult(
                transaction_id=self.qcc.transact_id,
                timestamp=datetime.now(),
                type="INSERT",
                status="error",
                query=query,
                previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
                new_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
            )


    def execute_create(self, query: str) -> ExecutionResult:
        try:
            schema_match = re.match(r"CREATE TABLE (\w+)\s*\((.+)\)", query, re.IGNORECASE)
            
            if not schema_match:
                raise ValueError("Error: Invalid CREATE TABLE statement.")
            
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
                    raise ValueError(f"Error: Invalid attribute definition '{attribute_str}'")

            self.storage_manager.create_table(table_name, Schema(attributes))
            
            timestamp = datetime.now()
            schema = self.storage_manager.get_table_schema(table_name)
            columns = [attr[0] for attr in schema.get_metadata()]

            new_data = Rows(data=[], rows_count=0, schema=schema, columns=columns)

            return ExecutionResult(
                transaction_id=self.qcc.transact_id,
                timestamp=timestamp,
                type="CREATE",
                status="success",
                query=query,
                previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
                new_data=new_data
            )

        except Exception as e:
            return ExecutionResult(
                transaction_id=self.qcc.transact_id,
                timestamp=datetime.now(),
                type="CREATE",
                status="error",
                query=query,
                previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
                new_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
            )

    def execute_update(self, query: str) -> ExecutionResult:
        try:
            update_match = re.match(r"UPDATE\s+(\w+)\s+SET\s+(.+)\s+WHERE\s+(.+)", query, re.IGNORECASE)

            if not update_match:
                raise ValueError("Error: Invalid UPDATE query.")

            table_name = update_match.group(1).strip().lower()
            set_clause = update_match.group(2).strip()
            where_clause = update_match.group(3).strip()

            update_values = {}
            for set_part in set_clause.split(','):
                set_part = set_part.strip()
                match = re.match(r"(\w+)\s*=\s*(.+)", set_part)
                if not match:
                    raise ValueError("Error: Invalid SET clause. Ensure it follows the correct format 'column = value'.")
                column_name = match.group(1).strip().lower()
                value = match.group(2).strip()
                update_values[column_name] = value

            match = re.match(r"(\w+)\s*([=<>!]+)\s*(.+)", where_clause)
            if not match:
                raise ValueError("Error: Invalid WHERE clause. Ensure it follows the correct format 'column operator value'.")

            where_column = match.group(1).strip().lower()
            operator = match.group(2).strip()
            where_value = match.group(3).strip()
            condition = Condition(where_column, operator, where_value)
            
            response = self.qcc.check_for_response_update(list(table_name))

            if (response != "OK"):
                res = ExecutionResult(
                    transaction_id=self.qcc.transact_id,
                    timestamp=datetime.now(),
                    type="UPDATE",
                    status="error",
                    query=query,
                    previous_data=Rows(data=[], rows_count=0, columns=[]),
                    new_data=Rows(data=[], rows_count=0, columns=[])
                )
                for rollback_query in response:
                    if self.qcc.is_transacting and self.qcc.is_rollingback:
                        if (get_query_type(rollback_query) == 'SELECT'):
                            self.execute_select(rollback_query)
                        elif (get_query_type(rollback_query) == 'INSERT'):
                            self.execute_insert(rollback_query)
                        elif (get_query_type(rollback_query) == 'UPDATE'):
                            self.execute_update(rollback_query)
                        elif (get_query_type(rollback_query) == 'DELETE'):
                            self.execute_delete(rollback_query)
                self.qcc.is_rollingback = False
                return res

            schema = self.storage_manager.get_table_schema(table_name)

            rows_affected = self.storage_manager.update_table(table_name, condition, update_values)
            print(f"{rows_affected} row(s) updated in '{table_name}'.")

            columns = [attr[0] for attr in schema.get_metadata()]
            timestamp = datetime.now()

            new_data = Rows(
                data=[],
                rows_count=0,
                schema=schema,
                columns=columns
            )

            res = ExecutionResult(
                transaction_id=self.qcc.transact_id,
                timestamp=timestamp,
                type="UPDATE",
                status="success",
                query=query,
                previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
                new_data=new_data
            )

            if (self.qcc.is_transacting):
                self.qcc.frm.write_log(res)

            return res

        except Exception as e:
            print(f"Error: {e}")
            return ExecutionResult(
                transaction_id=self.transact_id,
                timestamp=datetime.now(),
                type="UPDATE",
                status="error",
                query=query,
                previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
                new_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
            )

    def execute_delete(self, query: str) -> ExecutionResult:
        try:
            delete_match = re.match(r"DELETE FROM\s+(\w+)\s+WHERE\s+(.+)", query, re.IGNORECASE)

            if not delete_match:
                raise ValueError("Error: Invalid DELETE query.")

            table_name = delete_match.group(1).strip().lower()
            where_clause = delete_match.group(2).strip()

            match = re.match(r"(\w+)\s*([=<>!]+)\s*(.+)", where_clause)
            if not match:
                raise ValueError("Error: Invalid WHERE clause. Ensure it follows the correct format 'column operator value'.")

            where_column = match.group(1).strip().lower()
            operator = match.group(2).strip()
            where_value = match.group(3).strip()
            condition = Condition(where_column, operator, where_value)
            schema = self.storage_manager.get_table_schema(table_name)
            
            response = self.qcc.check_for_response_delete(list(table_name))

            if (response != "OK"):
                res = ExecutionResult(
                    transaction_id=self.qcc.transact_id,
                    timestamp=datetime.now(),
                    type="DELETE",
                    status="error",
                    query=query,
                    previous_data=Rows(data=[], rows_count=0, columns=[]),
                    new_data=Rows(data=[], rows_count=0, columns=[])
                )
                for rollback_query in response:
                    if self.qcc.is_transacting and self.qcc.is_rollingback:
                        if (get_query_type(rollback_query) == 'SELECT'):
                            self.execute_select(rollback_query)
                        elif (get_query_type(rollback_query) == 'INSERT'):
                            self.execute_insert(rollback_query)
                        elif (get_query_type(rollback_query) == 'UPDATE'):
                            self.execute_update(rollback_query)
                        elif (get_query_type(rollback_query) == 'DELETE'):
                            self.execute_delete(rollback_query)
                self.qcc.is_rollingback = False
                return res

            rows_affected = self.storage_manager.delete_table_record(table_name, condition)
            print(f"{rows_affected} row(s) deleted from '{table_name}'.")

            columns = [attr[0] for attr in schema.get_metadata()]
            timestamp = datetime.now()

            new_data = Rows(
                data=[],
                rows_count=0,
                schema=schema,
                columns=columns
            )

            res = ExecutionResult(
                transaction_id=self.transact_id,
                timestamp=timestamp,
                type="DELETE",
                status="success",
                query=query,
                previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
                new_data=new_data
            )

            if (self.qcc.is_transacting):
                self.qcc.frm.write_log(res)

            return res

        except Exception as e:
            print(f"Error: {e}")
            return ExecutionResult(
                transaction_id=self.transact_id,
                timestamp=datetime.now(),
                type="DELETE",
                status="error",
                query=query,
                previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
                new_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
            )

    def execute_drop(self, query: str) -> ExecutionResult:
        try:
            drop_match = re.match(r"DROP TABLE\s+(\w+)", query, re.IGNORECASE)

            if not drop_match:
                raise ValueError("Error: Invalid DROP TABLE statement.")

            table_name = drop_match.group(1).strip().lower()
            self.storage_manager.delete_table(table_name)

            timestamp = datetime.now()
            new_data = Rows(data=[], rows_count=0, schema=[], columns=[])

            return ExecutionResult(
                transaction_id=self.transact_id,
                timestamp=timestamp,
                type="DROP",
                status="success",
                query=query,
                previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
                new_data=new_data
            )

        except Exception as e:
            return ExecutionResult(
                transaction_id=self.transact_id,
                timestamp=datetime.now(),
                type="DROP",
                status="error",
                query=query,
                previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
                new_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
            )
    def execute_begin_transaction(self):
        self.qcc.begin_transaction()
        # res = ExecutionResult(
        #     transaction_id=self.qcc.transact_id,
        #     timestamp=datetime.now(),
        #     type="BEGIN TRANSACTION",
        #     status="success",
        #     query="BEGIN TRANSACTION",
        #     previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
        #     new_data=Rows(data=[], rows_count=0, schema=[], columns=[])
        # )
        # return res

    def execute_commit(self):
        self.qcc.end_transaction()
        # res = ExecutionResult(
        #     transaction_id=self.qcc.transact_id,
        #     timestamp=datetime.now(),
        #     type="COMMIT",
        #     status="success",
        #     query="COMMIT",
        #     previous_data=Rows(data=[], rows_count=0, schema=[], columns=[]),
        #     new_data=Rows(data=[], rows_count=0, schema=[], columns=[])
        # )
        # return res

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
                elif query_tree.child and query_tree.child[0].type == 'sort':
                    table_name = query_tree.child[0].child[0].val
                    table_data = self.storage_manager.get_table_data(table_name)
                    if table_data:
                        schema = self.storage_manager.get_table_schema(table_name)
                        columns = [attr[0] for attr in schema.get_metadata()]
                        sort_condition = query_tree.child[0].condition.split()
                        sort_column = sort_condition[0]
                        sort_order = sort_condition[1].lower() if len(sort_condition) > 1 else "asc"
                        column_indices = {attr[0]: idx for idx, attr in enumerate(schema.get_metadata())}
                        if sort_column in column_indices:
                            column_index = column_indices[sort_column]
                            reverse = sort_order == "desc"
                            result_data = sorted(table_data, key=lambda x: x[column_index], reverse=reverse)[:limit_value]
                        else:
                            print(f"Error: Column '{sort_column}' does not exist in table '{table_name}'.")
                    else:
                        print(f"No data found in table '{table_name}'.")
                elif query_tree.child and query_tree.child[0].type == 'sigma':
                    table_name = query_tree.child[0].child[0].val
                    where_clause = query_tree.child[0].condition
                    match = re.match(r"(\w+)\s*([=<>!]+)\s*(.+)", where_clause)
                    if match:
                        column_name = match.group(1)
                        operator = match.group(2)
                        value = match.group(3)
                        result_data = self.storage_manager.get_table_data(table_name, Condition(column_name, operator, value))[:limit_value]
                        schema = self.storage_manager.get_table_schema(table_name)
                        columns = [attr[0] for attr in schema.get_metadata()]
                    else:
                        print("Error: Invalid WHERE clause.")
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
                    elif query_tree.child[0].child and query_tree.child[0].child[0].type == 'sort':
                        table_name = query_tree.child[0].child[0].child[0].val
                        table_data = self.storage_manager.get_table_data(table_name)
                        if table_data:
                            schema = self.storage_manager.get_table_schema(table_name)
                            columns = [attr[0] for attr in schema.get_metadata()]
                            sort_condition = query_tree.child[0].child[0].condition.split()
                            sort_column = sort_condition[0]
                            sort_order = sort_condition[1].lower() if len(sort_condition) > 1 else "asc"
                            column_indices = {attr[0]: idx for idx, attr in enumerate(schema.get_metadata())}
                            if sort_column in column_indices:
                                column_index = column_indices[sort_column]
                                reverse = sort_order == "desc"
                                result_data = sorted(table_data, key=lambda x: x[column_index], reverse=reverse)[:limit_value]
                                print("result_data:", result_data)
                            else:
                                print(f"Error: Column '{sort_column}' does not exist in table '{table_name}'.")
                        else:
                            print(f"No data found in table '{table_name}'.")
                    else:
                        print("Error: No valid table node found under limit node.")
                        return [], [], {}
                elif query_tree.child and query_tree.child[0].type == 'sigma':
                    table_name = query_tree.child[0].child[0].val
                    where_clause = query_tree.child[0].condition
                    match = re.match(r"(\w+)\s*([=<>!]+)\s*(.+)", where_clause)
                    if match:
                        column_name = match.group(1)
                        operator = match.group(2)
                        value = match.group(3)
                        print("column_name:", column_name)
                        print("operator:", operator)
                        print("value:", value)

                        result_data = self.storage_manager.get_table_data(table_name, Condition(column_name, operator, value))
                        print(Condition(column_name, operator, value))
                        schema = self.storage_manager.get_table_schema(table_name)
                        columns = [attr[0] for attr in schema.get_metadata()]
                    else:
                        print("Error: Invalid WHERE clause.")
                        return [], [], {}
                elif query_tree.child and query_tree.child[0].type == 'sort':
                    table_name = query_tree.child[0].child[0].val
                    table_data = self.storage_manager.get_table_data(table_name)
                    if table_data:
                        schema = self.storage_manager.get_table_schema(table_name)

                        column_indices = {attr[0]: idx for idx, attr in enumerate(schema.get_metadata())}

                        sort_condition = query_tree.child[0].condition.split()
                        sort_column = sort_condition[0]
                        sort_order = sort_condition[1].lower() if len(sort_condition) > 1 else "asc"

                        if sort_column in column_indices:
                            column_index = column_indices[sort_column]

                            reverse = sort_order == "desc"

                            result_data = sorted(table_data, key=lambda x: x[column_index], reverse=reverse)

                            columns = [attr[0] for attr in schema.get_metadata()]
                            # print("result_data:", result_data)
                        else:
                            print(f"Error: Column '{sort_column}' does not exist in table '{table_name}'.")
                    else:
                        print(f"No data found in table '{table_name}'.")

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
                        result_data = sorted(table_data, key=lambda x: x[column_index], reverse=reverse)[:limit_value]
                    elif query_tree.child and query_tree.child[0].type == 'sort':
                        result_data = sorted(table_data, key=lambda x: x[column_index], reverse=reverse)
                    else:
                        result_data = table_data
                    
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

            elif query_tree.type == 'sigma':
                table_name = query_tree.child[0].val
                where_clause = query_tree.condition
    
                # Parsing where_clause menggunakan regex
                match = re.match(r"(\w+)\s*([=<>!]+)\s*(.+)", where_clause)
                if match:
                    column_name = match.group(1)
                    operator = match.group(2)
                    value = match.group(3)

                    print("column_name:", column_name)
                    print("operator:", operator)
                    print("value:", value)

                    result_data = self.storage_manager.get_table_data(table_name, Condition(column_name, operator, value))
                    schema = self.storage_manager.get_table_schema(table_name)
                    columns = [attr[0] for attr in schema.get_metadata()]
                else:
                    print("Error: Invalid WHERE clause.")
                    return [], [], {}
                
            elif query_tree.type == 'sort':
                if query_tree.child and query_tree.child[0].type == 'table':
                    table_name = query_tree.child[0].val
                    table_data = self.storage_manager.get_table_data(table_name)
                    if table_data:
                        schema = self.storage_manager.get_table_schema(table_name)

                        column_indices = {attr[0]: idx for idx, attr in enumerate(schema.get_metadata())}

                        sort_condition = query_tree.condition.split()
                        sort_column = sort_condition[0]
                        sort_order = sort_condition[1].lower() if len(sort_condition) > 1 else "asc"

                        if sort_column in column_indices:
                            column_index = column_indices[sort_column]

                            reverse = sort_order == "desc"

                            result_data = sorted(table_data, key=lambda x: x[column_index], reverse=reverse)

                            columns = [attr[0] for attr in schema.get_metadata()]
                            print("result_data:", result_data)
                        else:
                            print(f"Error: Column '{sort_column}' does not exist in table '{table_name}'.")
                    else:
                        print(f"No data found in table '{table_name}'.")
                else:
                    print("Error: No valid table node found under sort node.")




            return result_data, schema, columns

        except Exception as e:
            print(f"Error executing query: {e}")
            return [], [], {}

    def get_table_names(self, query_tree: QueryTree) -> list[str]:
        """
        Get all table names queried by user
        """
        table_names = []
        if query_tree.type == 'table':
            table_names.append(query_tree.val)
        elif query_tree.child:
            for child in query_tree.child:
                table_names.extend(self.get_table_names(child))
        return table_names