import sqlite3
import time
import logging
import os
from tabulate import tabulate # type: ignore


# Function to set up logging

def setup_logging(logfile):
    
    # Ensure the logs directory exists
    
    os.makedirs(os.path.abspath(os.path.join("logs")), exist_ok=True)
    
    logging.basicConfig(
        filename = logfile,
        level = logging.INFO,
        format = "%(asctime)s - %(levelname)s - %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S"
    )
    
    logging.info("Logging setup complete.")


# Function to set up SQLite database connection

def setup_sqlite():
    
    # Ensure the data directory exists
    
    os.makedirs(os.path.abspath(os.path.join("data")), exist_ok=True)

    # Connect to SQLite database (or create it if it doesn't exist)
    
    db_path = os.path.abspath(os.path.join("data", "my_database.db"))
    
    conn = sqlite3.connect(db_path)
    
    cursor = conn.cursor()
    
    return conn, cursor


# Function to execute SQL from a file and return results

def execute_sql_from_file(cursor, filepath):
    
    with open(filepath, "r") as file:
        
        sql_script = file.read()
        
    statements = sql_script.split(";")

    for statement in statements:
        
        cursor.execute(statement)


# Function to log execution details

def log_execution_details(start_time, action):
    
    end_time = time.time()
    
    duration = end_time - start_time  # log seconds with microseconds
    
    logging.info(f"{action} executed in {duration:.6f} seconds")


# Function to create tables

def create_tables(cursor, exercise):
    
    create_tables_relative_path = os.path.join("sql", f"{exercise}_create_tables.sql")
    
    create_tables_absolute_path = os.path.abspath(create_tables_relative_path)
    
    start_time = time.time()
    
    execute_sql_from_file(cursor, create_tables_absolute_path)
    
    log_execution_details(start_time, "Create Tables")


# Function to insert data

def insert_data(cursor, exercise):
    
    insert_data_relative_path = os.path.join("sql", f"{exercise}_insert_data.sql")
    
    insert_data_absolute_path = os.path.abspath(insert_data_relative_path)
    
    start_time = time.time()
    
    execute_sql_from_file(cursor, insert_data_absolute_path)
    
    log_execution_details(start_time, "Insert Data")


# Function to execute exercises and return results

def execute_exercise_queries(cursor, exercise):
    
    exercise_relative_path = os.path.join("sql", f"{exercise}.sql")
    
    exercise_absolute_path = os.path.abspath(exercise_relative_path)
    
    start_time = time.time()
    
    sql_script = f"DROP TABLE IF EXISTS {exercise};\n"
    
    sql_script += f"CREATE TABLE {exercise} AS\n"
    
    with open(exercise_absolute_path, "r") as file:
        
        exercise_query = file.read()
        
        sql_script += exercise_query
        
    statements = sql_script.split(";")

    for statement in statements:
                
        cursor.execute(statement)
    
    log_execution_details(start_time, f"Execute {exercise}")
    
    return exercise_query


# Function to create string with table content
    
def str_table_contents(cursor, table):
    
    cursor.execute(f"SELECT * FROM {table}")
    
    rows = cursor.fetchall()
    
    return tabulate(rows, headers=[description[0] for description in cursor.description], tablefmt="psql")


# Function to print table contents

def print_table_contents(cursor, tables):
    
    for table in tables:
        
        cursor.execute(f"SELECT * FROM {table}")
                
        print(f"Contents of table {table}:")
        
        print(str_table_contents(cursor, table))
        
        print("\n")


# Function to fetch and format results from queries

def fetch_and_format_results(cursor, exercise, question, question_text):
        
    # Execute the exercise queries
    
    exercise_query = execute_exercise_queries(cursor, exercise).replace("\n", "\n\t\t\t")
    
    formatted_result = f"\n\t{question}: {question_text}\n"
    
    formatted_result += f"\n\t\tBy executing this query:\n"
        
    formatted_result += f"\n\t\t{exercise_query}\n"
    
    formatted_result += f"\n\t\tThe results are:\n"
    
    formatted_result += "\n\t\t\t"
    
    formatted_result += str_table_contents(cursor, exercise).replace("\n", "\n\t\t\t")
    
    formatted_result += "\n"
        
    return formatted_result
