from QueryProcessor import QueryProcessor
from utils.result import ExecutionResult, print_execution_result

if __name__ == "__main__":
    base_path = "./db-test"
    query_processor = QueryProcessor(base_path)

    print("Welcome to KawulaSQL!")
    print("Enter your SQL query or type 'exit' to quit.\n")

    while True:
        query = input("Please enter your SQL query: ").strip()

        if query.lower() == "exit":
            print("Exiting KawulaSQL. Goodbye!")
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
