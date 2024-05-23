import warnings
import os

warnings.filterwarnings("ignore")

import sqlite3
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None) 

import sys
sys.path.append('../dota2')

import sqlite3
import os

def copy_database(source_db_name, target_db_name):
    src_conn = sqlite3.connect(source_db_name)
    dest_conn = sqlite3.connect(target_db_name)
    
    with dest_conn:
        src_conn.backup(dest_conn)
    
    src_conn.close()
    dest_conn.close()

copy_database('dot_dev.db', 'dot_hist_seventhirtyfive.db')