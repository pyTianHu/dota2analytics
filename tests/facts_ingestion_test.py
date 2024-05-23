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

from scripts.facts_ingestion import Publicmatches_Ingestion
from scripts.data_ingestion import *
from classes.tableoperations import TableOperations
from utils.utils import prepare_schema_for_df

def test_case_d10(db_name, table_name):
    conn = sqlite3.connect(db_name)

    query = f"SELECT * FROM {table_name}"

    df = pd.read_sql_query(query, conn)

    return len(df)



#pubs_obj = Publicmatches_Ingestion('dot_dev.db','publicmatches')
#pubs_obj.publicmatches_ingestion()

#print(test_case_d10('dot_dev.db','publicmatches'))


db_name = "dot_dev.db"
table_name = "publicmatches"
schema_str = prepare_schema_for_df(table_name)
patch_release_735 = 1702579663 # hardcoding 2023-12-14 16:07:43.429000+00:00 = 1702579663 as unixtimestamp into the select query => date of 7.35 patch release.

conn = sqlite3.connect(db_name)
cursor = conn.cursor()

# check if publicmatches table exists
table_object = TableOperations(db_name, table_name, schema_str)
exists = table_object.check_if_table_exists()

if not exists:
    table_object.create_table()


# once table is created, we get the max(start_time) available in the table to establish start_time variable
select_from_publicmatches_query =   f"""
                                        SELECT MAX(start_time) as start_time
                                        FROM publicmatches
                                    """

max_start_time_df = pd.read_sql_query(select_from_publicmatches_query, conn)
start_time = max_start_time_df['start_time'][0]

if start_time is None:
    start_time = patch_release_735

# getting data from api
query = f"""
        SELECT *
        FROM public_matches
        where start_time >= {start_time}
        and radiant_team IS NOT NULL
        and dire_team IS NOT NULL
        ORDER BY start_time ASC
        LIMIT 100000
    """
url = f"https://api.opendota.com/api/explorer?sql={query}"
response = requests.get(url, verify=False)
source_df = pd.DataFrame(response.json()['rows'])


#quality checks:
# duplicates in match_id column - if duplicates, drop
duplicates = source_df['match_id'].duplicated(keep='first').any()

# query match_id's from publicmatches, and drop match_id's that are already in publichmatches table
publicmatches_ids_query =   f""" 
                            SELECT match_id
                            FROM publicmatches
                            """

publicmatches_matchids = df = pd.read_sql_query(publicmatches_ids_query, conn)

deduplicated_source_df = source_df[~source_df['match_id'].isin(publicmatches_matchids['match_id'])]

# insert into target table
#insert_table_object = TableOperations(db_name, table_name, deduplicated_source_df)
# db_name = target database
# table_name = target table name
# data = df to insert => deduplicated_source_df

deduplicated_source_df['radiant_team'] = deduplicated_source_df['radiant_team'].apply(lambda x: ','.join(map(str, x)))
deduplicated_source_df['dire_team'] = deduplicated_source_df['dire_team'].apply(lambda x: ','.join(map(str, x)))
deduplicated_source_df.to_sql(table_name, conn, if_exists='append', index=False)


#print(f"len of publicmatches table is: {len(pubs_df)}")