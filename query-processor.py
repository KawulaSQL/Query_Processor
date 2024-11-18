from datetime import datetime
from typing import Generic, TypeVar, List

# Generic type for Rows<T>
T = TypeVar('T')

# Rows<T>
class Rows(Generic[T]):
    def __init__(self, data: List[T], rows_count: int):
        self.data = data
        self.rows_count = rows_count

# ExecutionResult
class ExecutionResult:
    def __init__(self, transaction_id: int, timestamp: datetime, message: str, data: Rows, query: str):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.message = message
        self.data = data
        self.query = query

# QueryProcessor
class QueryProcessor:
    def __init__(self):
        self.transaction_id_counter = 0

    def execute_query(self, query: str) -> ExecutionResult:
        """
        Execute a query and return an ExecutionResult.
        """
        self.transaction_id_counter += 1
        transaction_id = self.transaction_id_counter
        timestamp = datetime.now()

        # sending the query to the optimizer
        optimized_query_plan = self._optimize_query(query)
        if not optimized_query_plan:
            return ExecutionResult(
                transaction_id=transaction_id,
                timestamp=timestamp,
                message="Query optimization failed",
                data=Rows([], 0),
                query=query
            )

        # fetching data from storage
        rows = self._fetch_data(optimized_query_plan)

        # concurrency control
        if not self._check_concurrency_control(query):
            return ExecutionResult(
                transaction_id=transaction_id,
                timestamp=timestamp,
                message="Concurrency control failed",
                data=Rows([], 0),
                query=query
            )

        # log the transaction to the failure recovery manager
        self._log_transaction(transaction_id, query)

        # return the result
        return ExecutionResult(
            transaction_id=transaction_id,
            timestamp=timestamp,
            message="Query executed successfully",
            data=rows,
            query=query
        )

    def _optimize_query(self, query: str) -> str:
        """
        Simulate query optimization.
        Returns an optimized query plan or None if optimization fails.
        """
        return None

    def _fetch_data(self, query_plan: str) -> Rows:
        """
        Simulate data retrieval based on the query plan.
        """
        return Rows

    def _check_concurrency_control(self, query: str) -> bool:
        """
        Simulate concurrency control check.
        """
        return True

    def _log_transaction(self, transaction_id: int, query: str):
        """
        Simulate logging to the failure recovery manager.
        """
        print(f"Transaction {transaction_id} logged for query: {query}")

# Example Usage
# if __name__ == "__main__":
#     qp = QueryProcessor()
#     query = "SELECT * FROM users"
#     result = qp.execute_query(query)
    
#     print(f"Transaction ID: {result.transaction_id}")
#     print(f"Timestamp: {result.timestamp}")
#     print(f"Message: {result.message}")
#     print(f"Query: {result.query}")
#     print(f"Data: {result.data.data}")
#     print(f"Rows Count: {result.data.rows_count}")