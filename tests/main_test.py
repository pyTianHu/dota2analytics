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
from utils.bronze_utils import selected_columns
from utils.silver_utils import rows_isin

for table_name in selected_columns:
        cols = selected_columns.get(table_name)
        if len(cols) == 0:
            print(f"0 cols for {table_name} - {cols}")
        else:
            print(cols)