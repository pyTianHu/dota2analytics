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

patch = TableOperations(raw_db_name,"patch")
patch_date = patch.get_latest_patch()
active_from = patch_date['date'][0]

table = TableOperations(raw_db_name,table_name)
# df_fresh => dataframe to insert into bronze table
df_fresh = table.select_cols_to_df("bronze")
df_fresh['active_from'] = active_from
df_fresh['active_to'] = None
df_fresh['active_current'] = "TRUE"


# check if table exists
bronze_table = TableOperations(bronze_db_name, table_name)
if_exists = bronze_table.check_if_table_exists() 
if  if_exists == False:
    bronze_table.create_table()
    #if table does not exist, data load can happen here right away
    df_to_table = TableOperations(bronze_db_name, table_name, data = df_fresh)
    res = df_to_table.insert_df_into_table()
elif if_exists == True:
    #if table exists:
    # get current data from the table
    df_bronze = bronze_table.select_all_to_df()
    #print(df_bronze.head(10))
    # compare fields by id, other than the 3 SCD ones.  
    #columns_to_compare = df_bronze.columns.difference(['id', 'active_to', 'active_current'])
    #print(columns_to_compare)
    comparison_df = df_bronze.set_index('id').compare(df_fresh.set_index('id'), align_axis=1, keep_shape=True, keep_equal=True)
    print(comparison_df)

    #changed_ids = comparison_df.index.unique()

    #print(changed_ids)

    #new_records = df_fresh[df_fresh['id'].isin(changed_ids)]

    #print(new_records)
    #print(len(new_records))


    # if there is no change at all => skip insert
    # if there is a change => 
        # set source row active_to field to new row active_from field value
        # update source row active_current field to FALSE
        # append new row into source table
        # this results in the ID field having multiple values, so it cannot function as a primary key on the gold database => need to establish a new composite primary key for that layer.
        # JOINs will happen with the condition of active_current = True & also match start_time & patch release date
    

#df_to_table = TableOperations(bronze_db_name, table_name, data = df)
#res = df_to_table.insert_df_into_table()

#bronze_scd_df = df_to_table.select_all_to_df()

#print(bronze_scd_df)