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


from scripts.data_ingestion import *
from classes.tableoperations import TableOperations
from utils.utils import prepare_schema_for_df

class Facts_Ingestion():
    def __init__(self,db_name, table_name):
        self.db_name = db_name
        self.table_name = table_name
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def dedup_source(self, data, field_name):
        df = pd.DataFrame(data)
        df = df.drop_duplicates(subset=[field_name], keep='first')

        return df
    
    def dedup_source_to_target(self,data, field_name):
        ids_query =     f""" 
                        SELECT {field_name}
                        FROM {self.table_name}
                        """

        matchids = pd.read_sql_query(ids_query, self.conn)
        source_df = pd.DataFrame(data)

        deduplicated_source_df = source_df[~source_df['match_id'].isin(matchids['match_id'])]

        return deduplicated_source_df
        

    def publicmatches_ingestion(self):
        schema_str = prepare_schema_for_df(self.table_name)
        patch_release_735 = 1702579663 # hardcoding 2023-12-14 16:07:43.429000+00:00 = 1702579663 as unixtimestamp into the select query => date of 7.35 patch release.

        # check if publicmatches table exists
        table_object = TableOperations(db_name=self.db_name, table_name=self.table_name, schema=schema_str)
        exists = table_object.check_if_table_exists()

        if not exists:
            print("publicmatches table does not exist, creating it now")
            table_object.create_table()

        # once table is created, we get the max(start_time) available in the table to establish start_time variable
        select_from_publicmatches_query =   f"""
                                                SELECT MAX(start_time) as start_time
                                                FROM {self.table_name}
                                            """

        max_start_time_df = pd.read_sql_query(select_from_publicmatches_query, self.conn)
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
                LIMIT 150000
            """
        
        url = f"https://api.opendota.com/api/explorer?sql={query}"
        response = requests.get(url, verify=False)
        source_df = pd.DataFrame(response.json()['rows'])

        #quality checks:
        dedup_df_1 = self.dedup_source(source_df, 'match_id')
        dedup_df_2 = self.dedup_source_to_target(dedup_df_1, 'match_id')

        # insert into target table
        dedup_df_2['radiant_team'] = dedup_df_2['radiant_team'].apply(lambda x: ','.join(map(str, x)))
        dedup_df_2['dire_team'] = dedup_df_2['dire_team'].apply(lambda x: ','.join(map(str, x)))

        dedup_df_2.to_sql(self.table_name, self.conn, if_exists='append', index=False)