# KAWULASQL - Query Processor Component

## Table of Contents
* [General Info](#general-info)
* [Class Descriptions](#class-overview)
* [Technologies Used](#technologies-used)
* [Setup and Usage](#setup-and-usage)
* [Team Members](#team-members)

## **General Info**

This component integrates all components of the DBMS. It accepts a query as input, then executes it by processing it through multiple components.

## **Class Overview**

1. **QueryProcessor**
The `QueryProcessor` class serves as an intermediary to the `QueryExecutor` class. It identifies the type of the query, then executes it by calling the `QueryExecutor`. The functions of this class include:
    - process_query(str):
        - Identifies the type of the query from the query string.
        - Executes the query by calling the QueryExecutor according to the query type.

2. **QueryExecutor**
The function of the QueryExecutor class is to execute the query by communicating directly with the Query Optimizer, Storage Manager, Failure Recovery Manager, and Concurrency Control Manager components. The functions of this class include:
    - execute_select(str) -> ExecutionResult:
        - Identifies the type of the query from the query string.
        - Executes the query by calling the QueryExecutor according to the query type.
    - execute_select(str)  -> ExecutionResult:
        - Entry point to the execute_query function.
        - Parses the query by passing it to the QueryOptimizer component
        - Obtains the QueryTree from the parse result and passes it to the execute_query function.
    - execute_query(QueryTree)   -> Tuple[list, list, map]:
        - Iterates through a Query Tree and process each operation according to the type of node.
    - execute_insert(str)  -> ExecutionResult:
        - Parses and executes an insert query
        - Calls the Storage Manager component to insert the parsed list of records
    - execute_create(str)  -> ExecutionResult:
        - Parses and executes a Create Table query
        - Calls the Storage Manager component to create the parsed table and schema
    - execute_update(str)  -> ExecutionResult:
        - Parses and executes an UPDATE query
        - Calls the Storage Manager component to update the parsed list of records
    - execute_delete(str)  -> ExecutionResult:
        - Parses and executes a DELETE FROM query
        - Calls the Storage Manager component to delete the parsed list of records
    - execute_drop(str)    -> ExecutionResult:
        - Parses and executes a DROP query
        - Calls the Storage Manager to drop the parsed name of table

3. **Rows**
The Rows class stores a list of records/tuples/rows along with other necessary information. Attributes include:
    - **data**: Is the list of data containing the values of the record.
    - **rows_count**: Specifies the number of rows.
    - **schema**: Defines the schema for each records.
    - **columns**: Maps attributes/columns in the row to its type.
  
4. **ExecutionResult**
The ExecutionResult class stores information about the result of the query. Attributes include:
    - **transaction_id**: Specifies the id of the transaction.
    - **timestamp**: Specifies the timestamp of the transaction.
    - **status**: Specifies the status of the execution result (error/success).
    - **query**: Specifies the query string.
    - **previous_data**: Specifies the data before the execution of the query.
    - **new_data**: Specifies the data after the execution of the query.

## **Technologies Used**

- Python 3

## **Setup and Usage**

1. Clone this project:
```
https://github.com/KawulaSQL/Query_Processor.git
```

2. To run unit tests, execute the following command from the root directory:
```
python -m unittest UnitTest.py
```


## **Team Members**

| Nama            | NIM      | Workload |
| --------------- | -------- | -------- |
| Ariel Herfrison | 13522002 | unit test, client |
| Farhan Nafis Rayhan | 13522037 | cli, unit test, client |
| Randy Verdian | 13522067 | execute_select, execute_query |
| Marvin Scifo Y. Hutahaean | 13522110 | concurrency control Integration |
