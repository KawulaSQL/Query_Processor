from QueryExecutor import QueryExecutor
from utils.query import get_query_type

class QueryProcessor:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.query_executor = QueryExecutor(base_path)

    def process_query(self, query: str):
        """
        Process the query by determining its type and delegating execution.
        """
        try:
            query_type = get_query_type(query)
            
            if query_type == "SELECT":
                success = self.query_executor.execute_select(query)
            elif query_type == "INSERT":
                success = self.query_executor.execute_insert(query)
            elif query_type == "CREATE":
                success = self.query_executor.execute_create(query)
            elif query_type == "UPDATE":
                success = self.query_executor.execute_update(query)
            elif query_type == "DELETE":
                success = self.query_executor.execute_delete(query)
            elif query_type == "DROP":
                success = self.query_executor.execute_drop(query)
            elif query_type == "BEGIN TRANSACTION":
                self.query_executor.execute_begin_transaction()
                success = True
            elif query_type == "COMMIT":
                self.query_executor.execute_commit()
                success = True
            else:
                print(f"Unsupported query type: {query_type}")
                success = False

            return success

        except Exception as e:
            print(f"Error processing query: {e}")
            return False
