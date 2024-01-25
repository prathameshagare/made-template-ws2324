import pandas as pd
import sqlite3
import os

def fetch_data_from_sqlite(sql_filepath, table_name):
    # Connect to SQLite database
    conn = sqlite3.connect(sql_filepath)
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def test_data_existence(sql_filepath, table_name):
    df = fetch_data_from_sqlite(sql_filepath, table_name)
    if len(df) > 1:
        print(f"Data exists in {table_name} table.")
    else:
        print(f"Data does not exist in {table_name} table.")

def check_null_values(sql_filepath, table_name):
    df = fetch_data_from_sqlite(sql_filepath, table_name)
    if df.isnull().any().any():
        print(f"DataFrame for {table_name} contains null values.")
    else:
        print(f"DataFrame for {table_name} does not contain null values.")

def get_sql_file_path(directory, filename):
    return os.path.join(os.getcwd(), directory, filename)

def test_datasets():
    # Test 1: Accident Data
    accident_sql_filepath = get_sql_file_path("data", "accidentdata.sqlite")
    accident_table_name = "accidentdata"
    print(f"Testing data existence in {accident_table_name}...")
    test_data_existence(accident_sql_filepath, accident_table_name)
    print("Testing null values in accident dataset...")
    check_null_values(accident_sql_filepath, accident_table_name)

    # Test 2: Death Data
    death_sql_filepath = get_sql_file_path("data", "deathdata.sqlite")
    death_table_name = "deathdata"
    print(f"Testing data existence in {death_table_name}...")
    test_data_existence(death_sql_filepath, death_table_name)
    print("Testing null values in death dataset...")
    check_null_values(death_sql_filepath, death_table_name)
    
    print("All test runs completed :)")

if __name__ == "__main__":
    test_datasets()
