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
from utils.bronze_utils import selected_columns
from utils.silver_utils import rows_isin

db_name = "dot_dev_bronze.db"
table_name = "publicmatches"
raw_db_name = "dot_dev.db"
silver_db_name = "dot_dev_silver.db"

print(bronze_to_silver_transformation(db_name, table_name,silver_db_name))

#print(bronze_transformation(raw_db_name,table_name,db_name))
RAW_DB_DEV = "dot_dev.db"

#print(table_create_and_ingest(RAW_DB_DEV,table_name))