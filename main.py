from QueryProcessor import QueryProcessor
from bang.bangs import ExecutionResult  # Pastikan Anda mengimpor model ExecutionResult
from datetime import datetime


def print_execution_result(result: ExecutionResult):
    """Helper function to print ExecutionResult in a formatted manner."""
    print("Execution Result:")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Timestamp: {result.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Status: {result.status}")
    print(f"Query: {result.query}")
    print("\nPrevious Data:")
    if isinstance(result.previous_data, int):
        print(f"Previous Rows Count: {result.previous_data}")
    else:
        print(f"Rows Count: {result.previous_data.rows_count}")
        for row in result.previous_data.data:
            print(row)

    print("\nNew Data:")
    if isinstance(result.new_data, int):
        print(f"New Rows Count: {result.new_data}")
    else:
        print(f"Rows Count: {result.new_data.rows_count}")
        for row in result.new_data.data:
            print(row)
    print("-" * 50)


if __name__ == "__main__":
    base_path = "./db-test"
    query_processor = QueryProcessor(base_path)

    print("Welcome to the SQL Query Executor!")
    print("Enter your SQL query or type 'exit' to quit.\n")

    while True:
        query = input("Please enter your SQL query: ").strip()

        if query.lower() == "exit":
            print("Exiting the SQL Query Executor. Goodbye!")
            break

        print(f"\nOriginal Query: {query}\n")
        
        try:
            result = query_processor.process_query(query)
            if isinstance(result, ExecutionResult):
                print_execution_result(result)
            else:
                print("Unexpected result type.")
        except Exception as e:
            print(f"Error processing query: {str(e)}")
