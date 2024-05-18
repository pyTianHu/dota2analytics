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
from scripts.job import silver_to_gold_transformation

from utils.utils import open_schemas, logger
from utils.ingestion_utils import table_function_mapping
from utils.bronze_utils import selected_columns
from utils.silver_utils import rows_isin
from utils.gold_utils import table_rename, column_rename, selected_columns, table_constraints
from classes.dataframeoperations import DataFrameOperations


DEV_RAW_DB = "dot_dev.db"
DEV_BRONZE_DB = "dot_dev_bronze.db"
DEV_SILVER_DB = "dot_dev_silver.db"
DEV_GOLD_DB = "dot_dev_gold.db"


heroes = DataFrameOperations(DEV_SILVER_DB, 'patch')
df = heroes.silver_to_gold_df()

print(df)

#gold_table_object = TableOperations(db_name=DEV_GOLD_DB, table_name='f_patches', data = df)
#exists = gold_table_object.check_if_table_exists()

#insert_into = gold_table_object.insert_df_into_table()

#exists = gold_table_object.check_if_table_exists()
#return_df = gold_table_object.select_all_to_df()
