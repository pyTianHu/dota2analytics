import sys
sys.path.append('../dota2')

from classes.tableoperations import TableOperations
from data_ingestion import heroes_ingestion, herostats_ingestion
import sqlite3
from utils.utils import open_schemas, convert_list_to_string_df, prepare_schema_for_df, table_function_mapping

   
def table_create_and_ingest(db_name, table_name):

    #calling and executing ingestion function, storing it in df variable
    df = table_function_mapping.get(table_name)

    # Data transformation
    convert_list_to_string_df(df)

    #compile schema string 
    schema_str = prepare_schema_for_df(table_name)

    #instantiate object
    ingested_table = TableOperations(db_name, table_name, schema_str, df)

    #check whether table already exists
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
        return print(f"No data was inserted: {e}")

  