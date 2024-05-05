import sys
sys.path.append('../dota2')

import warnings
warnings.filterwarnings("ignore")

from classes.tableoperations import TableOperations
import sqlite3
from utils.utils import open_schemas, convert_list_to_string_df, prepare_schema_for_df, table_function_mapping, logger

   
def table_create_and_ingest(db_name, table_name):
    #call logger method with function name and df => source db, table => nothing, as no source yet, just call function to record start time
    tci = logger("table_create_and_ingest function started", "table_create_and_ingest")
    tci.new_or_existing_run
    #calling and executing ingestion function, storing it in df variable
    df = table_function_mapping.get(table_name)

    # Data transformation
    convert_list_to_string_df(df)

    #compile schema string 
    schema_str = prepare_schema_for_df(table_name)

    #instantiate object
    ingested_table = TableOperations(db_name, table_name, schema_str, df)

    #check whether table already exists => edit check, as it does not say the table was created
    exists = ingested_table.check_if_table_exists()
    print(exists)
    if exists:
        pass
    else:
        print(ingested_table.create_table())

    #execute insert
    # return has to be edited, as it returns invalid result. function runs successfully and that is what it returns.
    try:
        print(ingested_table.insert_df_into_table())
    except Exception as e:
        #call logger with function name and no data was inserted {e} exception
        return print(f"No data was inserted: {e}")
    
    tci2 = logger("table_create_and_ingest function finished", "table_create_and_ingest")
    tci2.new_or_existing_run


def bronze_transformation(db_name, table_name):
    # dropping unnecessary columns

    pass


def bronze_to_silver_transformation(db_name, table_name):

    pass


def silver_to_gold_transformation(db_name, table_name):

    pass