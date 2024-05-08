import sys
sys.path.append('../dota2')

import warnings
warnings.filterwarnings("ignore")

from classes.tableoperations import TableOperations
from classes.dataframeoperations import DataFrameOperations
from utils.utils import convert_list_to_string_df, prepare_schema_for_df, logger
from utils.ingestion_utils import table_function_mapping


def generic_table_creation_mechanism(db_name, table_name, data):
    try:
        table = TableOperations(db_name, table_name, data=data)
        
        #check if target table exists in db_name, if not, create:
        if table.check_if_table_exists() == True:
            pass
        else:
            table.create_table()

        # once the table exists, insert data:
        table.insert_df_into_table()
        return True

    except Exception as e:
        return False,e




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
    except Exception as e:
        #call logger with function name and no data was inserted {e} exception
        ndi = logger(f"{TableOperations.insert_df_into_table.__name__} function finished, no data was inserted into {table_name}")
        tci.new_or_existing_run()
    
    tci2 = logger("table_create_and_ingest function finished", "table_create_and_ingest")
    tci2.new_or_existing_run


def bronze_transformation(raw_db_name, table_name):
    # data source: raw layer; output: bronze layer => dot_dev_bronze.db for dev env, dot_bronze.db for prod
    # dropping unnecessary columns & selecting only necessary ones
    bt = logger(f"{bronze_transformation.__name__} function started", f"{bronze_transformation.__name__}")
    bt.new_or_existing_run()


    bronze_db_dev = "dot_dev_bronze.db"
    bronze_db_prod = "dot_bronze_prod.db"

    #get selected cols from utils
    # if selected cols is empty, pass
    # else proceed with selecting the data with provided cols, establish the new dataframe and save it into the bronze layer
    table = TableOperations(raw_db_name,table_name)
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

    bt2 = logger(f"{bronze_transformation.__name__} function finished: result of insert_df_into_table function => {res}", f"{bronze_transformation.__name__}")
    bt2.new_or_existing_run()

    return res



def bronze_to_silver_transformation(db_name, table_name):
    # row filters, generic transformations (upper, lower, anything general)

    #filtering and layer-specific can be done separately
    # we need a generic job that =>
        #checks if table exists
        #creates if not
        #inserts after

    # initialize dataframe
    # i need to use bronze db name and table name here, because that is the source.
    source_table = DataFrameOperations(db_name, table_name)

    # filters on source_df
    source_df = source_table.filter_rows()

    # save to silver layer
    silver_db_dev = "dot_dev_silver.db"

    #check if table exists in silver layer, if not, create it
    silver_table = TableOperations(silver_db_dev, table_name)
    
    if silver_table.check_if_table_exists() == False:
        silver_table.create_table()
    else:
        pass

    #insert into bronze table
    df_to_table = TableOperations(silver_db_dev, table_name, data = source_df)
    res = df_to_table.insert_df_into_table()

    #bt2 = logger(f"{bronze_transformation.__name__} function finished: result of insert_df_into_table function => {res}", f"{bronze_transformation.__name__}")
    #bt2.new_or_existing_run()

    return res


def silver_to_gold_transformation(db_name, table_name):


    pass