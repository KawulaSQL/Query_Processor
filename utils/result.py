from bang.bangs import ExecutionResult
from Storage_Manager.lib.Schema import Schema

def print_table(data, column_names, aliases):
    """Helper function to print a formatted table."""
    # Calculate column widths
    column_widths = [len(alias) for alias in aliases]
    for row in data:
        column_widths = [
            max(width, len(str(value)))
            for width, value in zip(column_widths, row)
        ]
    
    # Create row format and separator
    row_format = " | ".join(f"{{:<{width}}}" for width in column_widths)
    separator = "-+-".join("-" * width for width in column_widths)
    
    # Print table header and data
    print(row_format.format(*aliases))
    print(separator)
    for row in data:
        print(row_format.format(*row))

def process_columns_and_data(schema, columns, data):
    """Process columns and project data based on column names or aliases."""
    column_names = [attr[0] for attr in schema.get_metadata()]
    column_indices = []
    
    # Determine whether columns is a list or dictionary
    if isinstance(columns, list):
        aliases = columns
        for col in columns:
            if col in column_names:
                column_indices.append(column_names.index(col))
            else:
                raise ValueError(f"Error: Column '{col}' is not in table schema.")
    elif isinstance(columns, dict):
        aliases = list(columns.values())
        for col in columns.keys():
            if col in column_names:
                column_indices.append(column_names.index(col))
            else:
                raise ValueError(f"Error: Column '{col}' is not in table schema.")
    else:
        raise ValueError("Error: `columns` must be a list or a dictionary.")
    
    # Project data based on column indices
    projected_data = [[row[idx] for idx in column_indices] for row in data]
    return aliases, projected_data

def print_execution_result(result: ExecutionResult):
    """Helper function to print ExecutionResult in a formatted manner."""
    print("Execution Result:")
    print(f"Transaction ID: {result.transaction_id}")
    print(f"Timestamp: {result.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Status: {result.status}")
    print(f"Query: {result.query}")
    print(f"Type: {result.type}")
    
    print("\nPrevious Data:")
    if isinstance(result.previous_data, int):
        print(f"Previous Rows Count: {result.previous_data}")
    else:
        print(f"Rows Count: {result.previous_data.rows_count}")

    print("\nNew Data:")
    if isinstance(result.new_data, int):
        print(f"New Rows Count: {result.new_data}")
    else:
        print(f"Rows Count: {result.new_data.rows_count}")
        print("Schema:", result.new_data.schema)
        print("Columns:", result.new_data.columns)
        
        try:
            aliases, projected_data = process_columns_and_data(
                result.new_data.schema, result.new_data.columns, result.new_data.data
            )
            print_table(projected_data, result.new_data.columns, aliases)
        except ValueError as e:
            print(e)

    print("-" * 50)