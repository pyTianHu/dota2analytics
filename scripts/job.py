import sys
sys.path.append('../dota2')

import warnings
warnings.filterwarnings("ignore")

from classes.tableoperations import TableOperations
from classes.dataframeoperations import DataFrameOperations
from utils.utils import convert_list_to_string_df, prepare_schema_for_df, logger
from utils.ingestion_utils import table_function_mapping
from utils.gold_utils import table_constraints, table_rename
   
def table_create_and_ingest(db_name, table_name):
    #calling and executing ingestion function, storing it in df variable
    df = table_function_mapping.get(table_name)

    # Data transformation
    convert_list_to_string_df(df)

    #compile schema string 
    schema_str = prepare_schema_for_df(table_name)

    ingested_table = TableOperations(db_name, table_name, schema_str, df)

    #check whether table already exists => edit check, as it does not say the table was created
    exists = ingested_table.check_if_table_exists()

    if exists == True:
        pass
    else:
        ingested_table.create_table()


    #execute insert
    # return has to be edited, as it returns invalid result. function runs successfully and that is what it returns.
    try:
        ingested_table.insert_df_into_table()
        return True
    except Exception as e:
        return e


def bronze_transformation(raw_db_name, table_name, bronze_db_name):
    # data source: raw layer; output: bronze layer => dot_dev_bronze.db for dev env, dot_bronze.db for prod
    # dropping unnecessary columns & selecting only necessary ones
    

    #get selected cols from utils
    # if selected cols is empty, pass
    # else proceed with selecting the data with provided cols, establish the new dataframe and save it into the bronze layer
    table = TableOperations(raw_db_name,table_name)
    df = table.select_cols_to_df("bronze")

    #check if table exists in bronze, if not, create it
    bronze_table = TableOperations(bronze_db_name, table_name)
    
    if bronze_table.check_if_table_exists() == False:
        bronze_table.create_table()
    else:
        pass

    #insert into bronze table
    df_to_table = TableOperations(bronze_db_name, table_name, data = df)
    res = df_to_table.insert_df_into_table()

    return res



def bronze_to_silver_transformation(bronze_db_name, table_name, silver_db_name):
    # row filters, generic transformations (upper, lower, anything general)

    # initialize dataframe
    t = DataFrameOperations(bronze_db_name, table_name)

    # filters
    df = t.filter_rows()

    # create new tableoperations class with silver schema to create new table there and insert the data.
    silver_table = TableOperations(silver_db_name,table_name, data=df)
    res = silver_table.insert_df_into_table()

    return res


def silver_to_gold_transformation(silver_db_name, silver_table_name, gold_db_name, gold_table_name):

    # get source data from silver layer
    try:
        table = TableOperations(silver_db_name,silver_table_name)
        df = table.select_cols_to_df("gold")

        # commit transformation while data is in dataframe
        heroes = DataFrameOperations(silver_db_name, silver_table_name)
        df, table_renamed = heroes.silver_to_gold_df()

        # create table from dataframe on gold layer
        gold_table_object = TableOperations(db_name=gold_db_name, table_name=table_renamed, data=df)
        gold_table_object.insert_df_into_table()
        return True
    
    except Exception as e:
        return e