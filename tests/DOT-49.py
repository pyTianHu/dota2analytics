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
from scripts.job import table_create_and_ingest,bronze_transformation, bronze_to_silver_transformation

from utils.utils import open_schemas, logger
from utils.ingestion_utils import table_function_mapping
from utils.bronze_utils import bronze_selected_columns
from utils.silver_utils import rows_isin

raw_db_name = "dot_dev.db"
bronze_db_name = "dot_dev_bronze.db"
silver_db_name = "dot_dev_silver.db"

listoftables = [
    #'heroes',
    #'herostats',
    'publicmatches',
    #'abilities',
    #'ability_ids',
    'game_mode',
    #'hero_abilities',
    #'items_ids',
    #'items',
    'patch',
    'lobby_type'] 

conn = sqlite3.connect(silver_db_name)
cursor = conn.cursor()

for table in listoftables:
    query = f"SELECT * FROM {table} limit 5"

    df = pd.read_sql_query(query, conn)

    print(df)

