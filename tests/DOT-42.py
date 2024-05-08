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
from utils.silver_utils import rows_isin

db_name = "dot_dev_bronze.db"
table_name = "publicmatches"

df = bronze_to_silver_transformation(db_name, table_name)
print(df)