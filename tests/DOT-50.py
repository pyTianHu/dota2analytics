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
from scripts.job import bronze_transformation

from utils.utils import open_schemas, logger
from utils.ingestion_utils import table_function_mapping
from utils.bronze_utils import bronze_selected_columns
from utils.silver_utils import rows_isin, silver_selected_columns

def select_cols_to_df(layer, source_table_name, source_db_name):

    source_conn = sqlite3.connect(source_db_name)
    #source_cursor = source_conn.cursor()

    if layer == "bronze":
        cols = bronze_selected_columns.get(source_table_name)
    elif layer == "silver":
        cols = silver_selected_columns.get(source_table_name)
    
    coluns = ', '.join(cols)

    query = f'''
            SELECT {coluns}
            FROM {source_table_name}
            '''
    try:
        df = pd.read_sql_query(query, source_conn)
        return print(df)
    except Exception as e:
        return print(e)

listoftables = ['heroes',
    'herostats',
    'publicmatches',
    'abilities',
    'ability_ids',
    'game_mode',
    'hero_abilities',
    'item_ids',
    'items',
    'patch',
    'lobby_type'] 
for source_table_name in listoftables:
    select_cols_to_df(layer = 'bronze', source_table_name = source_table_name, source_db_name='dot_dev_bronze.db')