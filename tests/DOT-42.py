import warnings
import os

warnings.filterwarnings("ignore")

import sqlite3
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None) 
from datetime import datetime
import sys
sys.path.append('../dota2')

from classes.tableoperations import TableOperations
from classes.dataframeoperations import DataFrameOperations
from scripts.data_ingestion import *
from scripts.job import bronze_transformation, bronze_to_silver_transformation

from utils.utils import open_schemas, logger
from utils.ingestion_utils import table_function_mapping
from utils.bronze_utils import selected_columns
from utils.silver_utils import rows_isin, selected_columns as silver_selected_columns


bronze_db_name = "dot_dev_bronze.db"
silver_db_name = "dot_dev_silver.db"


def bronze_to_silver_transformation(bronze_db_name, table_name, silver_db_name):
    # row filters, generic transformations (upper, lower, anything general)

    # initialize dataframe
    t = DataFrameOperations(bronze_db_name, table_name)

    # filters
    df = t.filter_rows()

    #save df into silver layer via df_to_sql
    silver_table = TableOperations(silver_db_name,table_name, data=df)
    silver_table.insert_df_into_table()

    conn = sqlite3.connect(silver_db_name)
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql_query(query, conn)
    

    return print(table_name, len(df))

for table_name in silver_selected_columns:
    print(bronze_to_silver_transformation(bronze_db_name, table_name, silver_db_name))