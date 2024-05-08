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
table_name = "game_mode"

#df = bronze_to_silver_transformation(db_name, table_name)
#print(df)

data = {
    'name': ['7.30', '7.31', '7.32', '7.33', '7.34', '7.35'],
    'id': [1, 2, 3, 4, 5, 6],
    'date': [
        datetime(2022, 1, 1),
        datetime(2022, 6, 1),
        datetime(2023, 1, 1),
        datetime(2023, 6, 1),
        datetime(2024, 1, 1),
        datetime(2024, 6, 1)
    ]
}

df = pd.DataFrame(data)


filter_criteria = rows_isin[table_name]
#print(filter_criteria)
for field_name, criteria in filter_criteria.items():
    #print(field_name)
    for action, values in criteria.items():
        if action == "drop":
            df = df[~df[field_name].isin(values)]
        else:
            df = df[df[field_name].isin(values)]


print(df)