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

#establish variables
DEV_GOLD_DB = "dot_dev_gold.db"

# query data from gold layer, establish dfs
heroes = TableOperations(db_name=DEV_GOLD_DB,table_name='dim_heroes')
dim_heroes = heroes.select_all_to_df()

game_modes = TableOperations(db_name=DEV_GOLD_DB, table_name='dim_game_modes')
dim_game_modes = game_modes.select_all_to_df()

patch = TableOperations(db_name=DEV_GOLD_DB, table_name='dim_patches')
dim_patch = patch.select_all_to_df()

lobby_types = TableOperations(db_name=DEV_GOLD_DB, table_name='dim_lobby_types')
dim_lobby_types = lobby_types.select_all_to_df()

pubs = TableOperations(db_name=DEV_GOLD_DB, table_name='f_pubs')
f_pubs = pubs.select_all_to_df()


#join dataframes together, drop unnecessary rows
pubs_game_modes = pd.merge(f_pubs, dim_game_modes, left_on='game_mode_id', right_on='game_modes_id', how='inner').drop(columns={'game_mode_id','game_modes_id'})

pubs_modes_lobbies = pd.merge(pubs_game_modes, dim_lobby_types, on= 'lobby_type_id', how='inner').drop(columns={'lobby_type_id'})

# joining heroes table with pubs => choosing a different option than merge due to performance and technical reasons
pubs_modes_lobbies['radiant_team_heroes'] = pubs_modes_lobbies['radiant_team_heroes'].str.split(',')
pubs_modes_lobbies['dire_team_heroes'] = pubs_modes_lobbies['dire_team_heroes'].str.split(',')

hero_id_to_name = dict(zip(dim_heroes['heroes_id'], dim_heroes['hero_name']))

def map_hero_names(hero_ids):
    #maps hero ids to hero names
    return [hero_id_to_name.get(int(hero_id)) for hero_id in hero_ids]

pubs_modes_lobbies['radiant_hero_names'] = pubs_modes_lobbies['radiant_team_heroes'].apply(map_hero_names)
pubs_modes_lobbies['dire_hero_names'] = pubs_modes_lobbies['dire_team_heroes'].apply(map_hero_names)

pubs_modes_lobbies_heroes = pubs_modes_lobbies.drop(columns={'dire_team_heroes', 'radiant_team_heroes'})

pubs_modes_lobbies_heroes['winning_team'] = pubs_modes_lobbies_heroes['winning_team'].apply(lambda x: "Radiant" if x == 1 else "Dire")

pubs_modes_lobbies_heroes['game_start_timestamp'] = pd.to_datetime(pubs_modes_lobbies_heroes['game_start_timestamp']).dt.tz_localize(None)
dim_patch['patch_release_date'] = pd.to_datetime(dim_patch['patch_release_date']).dt.tz_localize(None)

pubs_modes_lobbies_sorted = pubs_modes_lobbies_heroes.sort_values('game_start_timestamp')
dim_patch_sorted = dim_patch.sort_values('patch_release_date')

# Perform an asof merge to match each game_start_timestamp with the nearest patch_release_date
merged_data = pd.merge_asof(
    pubs_modes_lobbies_sorted,
    dim_patch_sorted,
    left_on='game_start_timestamp',
    right_on='patch_release_date',
    direction='backward',
    suffixes=('', '_patch')
)

# Drop unnecessary columns
merged_data.drop(columns=['patch_id', 'patch_release_date'], inplace=True)

print(merged_data.head(5))