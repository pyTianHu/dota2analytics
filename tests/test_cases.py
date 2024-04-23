import sqlite3
import pandas as pd
import sys
sys.path.append('../dota2')

from classes.tableoperations import TableOperations

def test_case_d10(db_name, table_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    query = f"SELECT * FROM {table_name}"

    df = pd.read_sql_query(query, conn)

    print(df)

#test_case_d10('dot_raw_prod.db', 'heroes')

def test_case_dot5(db_name, table_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    #create test table
    schema = ('id INTEGER NOT NULL, name TEXT NULL')
    test_table = TableOperations(db_name, table_name)
    test_table.create_table()

    #check if table exists
    check_table_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    cursor.execute(check_table_query)

    # Fetch the result
    result = cursor.fetchone()

    # Check if the table exists
    if result:
        print(f"The table '{table_name}' exists in the database.")
    else:
        print(f"The table '{table_name}' does not exist in the database.")

    #drop table
    test_table.drop_table()

    #check if table exists
    check_table_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    cursor.execute(check_table_query)

    # Fetch the result
    result = cursor.fetchone()

    # Check if the table exists
    if result:
        print(f"The table '{table_name}' exists in the database.")
    else:
        print(f"The table '{table_name}' does not exist in the database.")

#test_case_dot5('dot_dev.db','test_table')

def test_case_dot13():
    table_name = 'random'
    existing_database = 'dot_raw_prod.db'
    random_table = TableOperations(existing_database, table_name)
    print(random_table.check_if_table_exists())

    existing_table = 'heroes'
    existing = TableOperations(existing_database, existing_table)
    print(existing.check_if_table_exists())

test_case_dot13()