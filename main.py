from QueryProcessor import QueryProcessor

if __name__ == "__main__":
    base_path = "./db-test"
    query_processor = QueryProcessor(base_path)

    query = input("Please enter your SQL query: ")
    print(f"Original Query: {query}\n")
    query_processor.process_query(query)