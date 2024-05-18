import warnings
import os

warnings.filterwarnings("ignore")

import sqlite3
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None) 
import json
import sys
sys.path.append('../dota2')

from classes.tableoperations import TableOperations

from scripts.data_ingestion import *
from scripts.job import bronze_transformation

from utils.utils import open_schemas, logger
from utils.ingestion_utils import table_function_mapping
from utils.bronze_utils import selected_columns
from utils.silver_utils import rows_isin

listoftables = [
    'heroes',
    'herostats',
    'publicmatches',
    'abilities',
    'ability_ids',
    'game_mode',
    'hero_abilities',
    'items_ids',
    'items',
    'patch',
    'lobby_type'] 

goldtables = [
    'dim_heroes',
    'f_pubs',
    'f_game_modes',
    'f_patches',
    'f_lobby_types'
    ] 

silvertables = [
    'heroes',
    'publicmatches',
    'game_mode',
    'patch',
    'lobby_type'
    ] 

def test_case_d10(db_name, table_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()


    query = f"SELECT * FROM {table_name}"

    df = pd.read_sql_query(query, conn)

    print(f"{table_name}, \n, {df.head()}")

for table in goldtables:
    test_case_d10('dot_dev_gold.db',table)

#scols = TableOperations('dot_dev.db','publicmatches')
#print(scols.select_cols_to_df())

def test_case_dot5(db_name, table_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    table = TableOperations(db_name, table_name)
    

    #check if table exists
    #check_table_query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    #cursor.execute(check_table_query)

    # Fetch the result
    #result = cursor.fetchone()

    ## Check if the table exists
    #if result:
    #    print(f"The table '{table_name}' exists in the database.")
    #else:
    #    print(f"The table '{table_name}' does not exist in the database.")

    #drop table
    table.drop_table()

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


#for table in goldtables:
#    test_case_dot5('dot_dev_gold.db',table)

#print(bronze_transformation('dot_dev.db','heroes'))

def test_case_dot13():
    table_name = 'random'
    existing_database = 'dot_raw_prod.db'
    random_table = TableOperations(existing_database, table_name)
    print(random_table.check_if_table_exists())

    existing_table = 'heroes'
    existing = TableOperations(existing_database, existing_table)
    print(existing.check_if_table_exists())

#test_case_dot13()

def test_case_dot6():
    db_name = "dot_dev.db"
    table_name = "test_table"
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    #create dataframe
    data = {
            'id': [1, 2, 3],
            'name': ['Pudge', 'Anti Mage', 'Snapfire']
            }
    df = pd.DataFrame(data)

    #create table in test environment
    schema = ('id INTEGER NOT NULL, name TEXT NULL')
    test_table = TableOperations(db_name, table_name, schema, df)
    test_table.create_table()
    
    
    #insert into test table
    test_table.insert_df_into_table()

    #select * from table
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    print(df)

    #delete test table
    test_table.drop_table()

#test_case_dot6()

def test_case_dot6_2():
    db_name = 'dot_raw_prod.db'
    table_name = 'heroes'

    conn = sqlite3.connect(db_name)

    #select * from table
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    print(df)

#test_case_dot6_2()

def test_case_dot15():
    
    df = herostats_ingestion()
    return df

#df = test_case_dot15()
#print(df.info())


def test_case_dot6_2():
    db_name = 'dot_dev.db'
    table_name = 'publicmatches'

    conn = sqlite3.connect(db_name)

    #select * from table
    query = f"SELECT * FROM {table_name} limit 10"
    df = pd.read_sql_query(query, conn)
    print(df)

#test_case_dot6_2()

def test_case_dot24():
    db_name = 'dot_dev.db'
    table_name = 'publicmatches'

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    query = f"DELETE FROM {table_name}"
    cursor.execute(query)
    conn.commit()

    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    print(df)


#test_case_dot24()

def test_case_dot26():
    df = publicmatches_ingestion()
    return print(df)

#test_case_dot26()


def test_case_prepschema():
    #data = prepare_schema_for_df("item_ids")

    table_name = "items_ids"

    schemas = open_schemas()
    data_table = [table for table in schemas.get("tables", []) if table.get("name") == table_name]

    return print(data_table)

#print(test_case_prepschema())

#l1 = logger("6 testing again", "6 new functin name")
#print(l1.new_or_existing_run())

#conn = sqlite3.connect('dot_dev.db')
#cursor = conn.cursor()
#cursor.execute("select name from sqlite_master WHERE type = 'table';")
#tables = cursor.fetchall()
#for table in tables:
    #print(table)
    #query = f"drop table {table[0]}"
#    query = f"select * from {table[0]} limit 1"
#    cursor.execute(query)
#    conn.commit
#    result = cursor.fetchone()
#    print(table, "\n \n \t", result)

#cursor.execute("select name from sqlite_master WHERE type = 'table';")
#tables = cursor.fetchall()
#print(tables)





