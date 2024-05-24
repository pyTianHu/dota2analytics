import warnings
import os

warnings.filterwarnings("ignore")

import sqlite3
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None) 
import sys
sys.path.append('../dota2')

from classes.tableoperations import TableOperations

from scripts.data_ingestion import *

raw_db_name = 'dot_dev.db'
bronze_db_name = 'dot_dev_bronze.db'
table_name = 'heroes'

table = TableOperations(raw_db_name,table_name)
df = table.select_cols_to_df("bronze")

patch = TableOperations(raw_db_name,"patch")
patch_date = patch.get_latest_patch()

active_from = patch_date['date'][0]

df['active_from'] = active_from
df['active_to'] = None
df['active_current'] = "TRUE"

print(df.head())

bronze_table = TableOperations(bronze_db_name, table_name)
    
if bronze_table.check_if_table_exists() == False:
    bronze_table.create_table()

#df_to_table = TableOperations(bronze_db_name, table_name, data = df)
#res = df_to_table.insert_df_into_table()

#bronze_scd_df = df_to_table.select_all_to_df()

#print(bronze_scd_df)