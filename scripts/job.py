import sys
sys.path.append('../dota2')

import warnings
warnings.filterwarnings("ignore")

from classes.tableoperations import TableOperations
from classes.dataframeoperations import DataFrameOperations
from utils.utils import convert_list_to_string_df, prepare_schema_for_df, logger
from utils.ingestion_utils import table_function_mapping


def generic_table_creation_mechanism(source_db_name = None, target_db_name = None, source_table_name = None, target_table_name = None, schema = None, data = None):
    
    #db_name = source database
    #table_name = table_name (both in source and in target)
    #data = data to insert to target
    #target_table_name
    
    gtcm = logger(f"{generic_table_creation_mechanism.__name__} function started", f"{generic_table_creation_mechanism.__name__}")
    gtcm.new_or_existing_run()

    try:
        table = TableOperations(source_db_name, target_db_name, source_table_name, target_table_name, schema, data)
        
        #check if target table exists in target_db_name, if not, create target_table_name in target_db_name:
        if table.check_if_table_exists() == True:
            pass
        else:
            table.create_table()

        # once target_table_name exists, insert data:
        table.insert_df_into_table()
        
        gtcm = logger(f"{generic_table_creation_mechanism.__name__} function finished with result:{True}", f"{generic_table_creation_mechanism.__name__}")
        gtcm.new_or_existing_run()
        
        return True

    except Exception as e:
        gtcm = logger(f"{generic_table_creation_mechanism.__name__} function failed with result: {False, e}", f"{generic_table_creation_mechanism.__name__}")
        gtcm.new_or_existing_run()
        return False


def table_create_and_ingest(raw_db_name, table_name):
    #source => API ; target => RAW layer
    tci = logger(f"{table_create_and_ingest.__name__} function started", f"{table_create_and_ingest.__name__}")
    tci.new_or_existing_run()

    # get source data from API
    df = table_function_mapping.get(table_name)

    # Data transformation on source data
    source_data = convert_list_to_string_df(df)

    #compile schema string 
    schema_str = prepare_schema_for_df(table_name)
    
    #params to pass to generic_table_creation_mechanism: target_db_name = raw_db_name, target_table_name = None, schema = None, 
    res = generic_table_creation_mechanism(target_db_name = raw_db_name, target_table_name = table_name, schema=schema_str, data = source_data)


    #next step is to set the tableoperations call in each generic_table... function, make sure all parameters are passed properly and all source and target databases and tables are passed properly

    return res


def bronze_transformation(raw_db_name, table_name, bronze_db_name):
    # data source: raw layer; output: bronze layer => dot_dev_bronze.db for dev env, dot_bronze.db for prod
    # dropping unnecessary columns & selecting only necessary ones

    bt = logger(f"{bronze_transformation.__name__} function started", f"{bronze_transformation.__name__}")
    bt.new_or_existing_run()

    #get source data
    table = TableOperations(raw_db_name,table_name)
    data = table.select_cols_to_df('bronze')

    #params to pass to generic_table_creation_mechanism: target_db_name = raw_db_name, target_table_name = None, schema = None, 
    res = generic_table_creation_mechanism(source_db_name = raw_db_name, target_db_name = bronze_db_name, source_table_name= table_name, target_table_name = table_name, schema=None, data = data)

    bt2 = logger(f"{bronze_transformation.__name__} function finished: result of insert_df_into_table function => {res}", f"{bronze_transformation.__name__}")
    bt2.new_or_existing_run()

    return res


def bronze_to_silver_transformation(bronze_db_name, table_name, silver_db_name):
    # row filters, generic transformations (upper, lower, anything general)

    bts = logger(f"{bronze_to_silver_transformation.__name__} function started", f"{bronze_to_silver_transformation.__name__}")
    bts.new_or_existing_run()

    # get source data - i need to use bronze db name and table name here, because that is the source.
    source_table = DataFrameOperations(bronze_db_name, table_name)

    # filters on source_df
    data = source_table.filter_rows()

    # save to silver layer
    res = generic_table_creation_mechanism(source_db_name = bronze_db_name, target_db_name = silver_db_name, source_table_name = table_name, target_table_name = table_name, schema=None, data = data)

    bts = logger(f"{bronze_to_silver_transformation.__name__} function finished", f"{bronze_to_silver_transformation.__name__}")
    bts.new_or_existing_run()

    return res

    
def silver_to_gold_transformation(db_name, table_name):


    pass