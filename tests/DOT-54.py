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
from utils.gold_utils import table_rename


DEV_RAW_DB = "dot_dev.db"
DEV_BRONZE_DB = "dot_dev_bronze.db"
DEV_SILVER_DB = "dot_dev_silver.db"
DEV_GOLD_DB = "dot_dev_gold.db"

    # Silver to gold
#for silver_table_name, gold_table_name in table_rename.items():
    #table_name will be the source table_name to use from silver => silver_table_name
    #gold_table_name will be 
    #silver_to_gold_transformation(silver_db_name, silver_table_name, gold_db_name, gold_table_name)
    
#    print(silver_to_gold_transformation(DEV_SILVER_DB, silver_table_name, DEV_GOLD_DB, gold_table_name))

def transformations(table_name):

        def dim_heroes(table_name):
            return print('dim_heroes')

        def f_pubs(table_name):
            return print('f_pubs')

        def f_game_modes(table_name):
            return print('f_game_modes')

        def f_patches(table_name):
            return print('f_patches')

        def f_lobby_types(table_name):
            return print('f_lobby_types')

     