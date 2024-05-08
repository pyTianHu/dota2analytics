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

log_directory = "logs"

ongoing_file = f"2024-05-09_2_ongoing.txt"

new_file_name = ongoing_file.replace("_ongoing","")
print(new_file_name)

nfn = os.rename(f"logs/{ongoing_file}", f"logs/{new_file_name}")
print(nfn)


