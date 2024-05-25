import warnings
warnings.filterwarnings("ignore")

import sqlite3

import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None) 
pd.set_option('display.max_colwidth', None)

import sys
sys.path.append('../dota2')

from classes.tableoperations import TableOperations
from classes.dataframeoperations import DataFrameOperations


class Pubs_Prep_DF():
    def __init__(self, db_name):
        self.db_name = db_name
        self.dim_heroes = TableOperations(db_name=self.db_name,table_name='dim_heroes').select_all_to_df()
        self.dim_game_modes = TableOperations(db_name=self.db_name,table_name='dim_game_modes').select_all_to_df()
        self.dim_patch = TableOperations(db_name=self.db_name,table_name='dim_patches').select_all_to_df()
        self.dim_lobby_types = TableOperations(db_name=self.db_name,table_name='dim_lobby_types').select_all_to_df()
        self.f_pubs = TableOperations(db_name=self.db_name,table_name='f_pubs').select_all_to_df()
        

    def main_line(self):
        def map_hero_names(hero_ids):
            #maps hero ids to hero names
            return [hero_id_to_name.get(int(hero_id)) for hero_id in hero_ids]
        
        pubs_game_modes = pd.merge(self.f_pubs, self.dim_game_modes, left_on='game_mode_id', right_on='game_modes_id', how='inner').drop(columns={'game_mode_id','game_modes_id'})
        pubs_modes_lobbies = pd.merge(pubs_game_modes, self.dim_lobby_types, on= 'lobby_type_id', how='inner').drop(columns={'lobby_type_id'})

        # joining heroes table with pubs => choosing a different option than merge due to performance and technical reasons
        hero_id_to_name = dict(zip(self.dim_heroes['heroes_id'], self.dim_heroes['hero_name']))
        pubs_modes_lobbies['radiant_team_heroes'] = pubs_modes_lobbies['radiant_team_heroes'].str.split(',').apply(map_hero_names)
        pubs_modes_lobbies['dire_team_heroes'] = pubs_modes_lobbies['dire_team_heroes'].str.split(',').apply(map_hero_names)
        
        # Joining patches to pubs to get patch name based on game_start_timestamp
        pubs_modes_lobbies['game_start_timestamp'] = pd.to_datetime(pubs_modes_lobbies['game_start_timestamp']).dt.tz_localize(None)
        self.dim_patch['patch_release_date'] = pd.to_datetime(self.dim_patch['patch_release_date']).dt.tz_localize(None)

        # Perform an asof merge to match each game_start_timestamp with the nearest patch_release_date
        pubs_modes_lobbies_heroes_sorted = pubs_modes_lobbies.sort_values('game_start_timestamp')
        dim_patch_sorted = self.dim_patch.sort_values('patch_release_date')

        merged_data = pd.merge_asof(
            pubs_modes_lobbies_heroes_sorted,
            dim_patch_sorted,
            left_on='game_start_timestamp',
            right_on='patch_release_date',
            direction='backward',
            suffixes=('', '_patch')
        )

        merged_data.drop(columns=['patch_id', 'patch_release_date'], inplace=True)


        # Any other necessary transformations
        merged_data['winning_team'] = merged_data['winning_team'].apply(lambda x: "Radiant" if x == 1 else "Dire")
        merged_data['game_mode_name'] = merged_data['game_mode_name'].apply(lambda x: 'All Pick' if x == "game_mode_all_draft" else x)
        merged_data['lobby_type_name'] = merged_data['lobby_type_name'].apply(lambda x: 'Ranked' if x == 'lobby_type_ranked' else x)
        merged_data['radiant_team_heroes'] = merged_data['radiant_team_heroes'].apply(lambda x: ','.join(map(str, x)))
        merged_data['dire_team_heroes'] = merged_data['dire_team_heroes'].apply(lambda x: ','.join(map(str, x)))
        
        #Insert into gold database table db_name, table_name, schema = None, data = None
        df_object = TableOperations(db_name=self.db_name, table_name='pubs_prep', data=merged_data)
        
        if not df_object.check_if_table_exists():
            df_object.create_table()
        #if exists, check for data differences, and append only the different data (in future release)- for now, it's just replace
        df_object.insert_df_into_table()