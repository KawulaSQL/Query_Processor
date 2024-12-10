from bang.bangs import ExecutionResult
from Storage_Manager.lib.Schema import Schema

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

        column_names = [attr[0] for attr in result.new_data.columns.get_metadata()]
        
        column_widths = [max(len(str(attr[0])), max(len(str(row[i])) for row in result.new_data.data)) for i, attr in enumerate(result.new_data.columns.get_metadata())]

        header = " | ".join([f"{col_name:{column_widths[i]}}" for i, col_name in enumerate(column_names)])
        print(f"\n{header}")
        print("-" * (len(header)))

        for row in result.new_data.data:
            print(" | ".join([f"{str(cell):<{column_widths[i]}}" for i, cell in enumerate(row)]))

    print("-" * 50)