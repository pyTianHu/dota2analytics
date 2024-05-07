import sys
sys.path.append('../dota2')

import warnings
warnings.filterwarnings("ignore")

from classes.tableoperations import TableOperations
import sqlite3
from utils.utils import open_schemas, convert_list_to_string_df, prepare_schema_for_df, table_function_mapping, logger, selected_columns

   
def table_create_and_ingest(db_name, table_name):
    #call logger method with function name and df => source db, table => nothing, as no source yet, just call function to record start time
    tci = logger(f"{table_create_and_ingest.__name__} function started", f"{table_create_and_ingest.__name__}")
    tci.new_or_existing_run()
    #calling and executing ingestion function, storing it in df variable
    df = table_function_mapping.get(table_name)

    # Data transformation
    convert_list_to_string_df(df)

    #compile schema string 
    schema_str = prepare_schema_for_df(table_name)

    #instantiate object
    ingested_table = TableOperations(db_name, table_name, schema_str, df)

    #check whether table already exists => edit check, as it does not say the table was created
    cite = logger("check_if_table_exists function started", "table_create_and_ingest")
    cite.new_or_existing_run()
    exists = ingested_table.check_if_table_exists()


    if exists:
        pass
    else:
        ingested_table.create_table()
        ct = logger("create_table function started")

    #execute insert
    # return has to be edited, as it returns invalid result. function runs successfully and that is what it returns.
    try:
        ingested_table.insert_df_into_table()
    except Exception as e:
        #call logger with function name and no data was inserted {e} exception
        return print(f"No data was inserted: {e}")
    
    tci2 = logger("table_create_and_ingest function finished", "table_create_and_ingest")
    tci2.new_or_existing_run


def bronze_transformation(db_name, table_name):
    # data source: raw layer; output: bronze layer => dot_dev_bronze.db for dev env, dot_bronze.db for prod
    # dropping unnecessary columns & selecting only necessary ones

    bronze_db_dev = "dot_dev_bronze.db"
    bronze_db_prod = "dot_bronze_prod.db"

    #get selected cols from utils
    # if selected cols is empty, pass
    # else proceed with selecting the column, establishing the new dataframe and saving it into the silver_layer
    

    table = TableOperations(db_name,table_name)
    df = table.select_cols_to_df()

    #check if table exists in bronze, if not, create it
    bronze_table = TableOperations(bronze_db_dev, table_name)
    
    if bronze_table.check_if_table_exists() == False:
        bronze_table.create_table()
    else:
        pass

    #insert into bronze table
    df_to_table = TableOperations(bronze_db_dev, table_name, data = df)
    res = df_to_table.insert_df_into_table()

    return res




def bronze_to_silver_transformation(db_name, table_name):
    # row filters, generic transformations (upper, lower, anything general)

    pass


def silver_to_gold_transformation(db_name, table_name):


    pass