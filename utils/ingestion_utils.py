import sys
sys.path.append('../dota2')

from scripts.data_ingestion import *

table_function_mapping = {
        'patch': patch_ingestion(),
        'heroes': heroes_ingestion(),
        'herostats': herostats_ingestion(),
        'abilities': abilities_ingestion(),
        'ability_ids': ability_ids_ingestion(),
        'game_mode': game_mode_ingestion(),
        'hero_abilities': hero_abilities_ingestion(),
        'items_ids': items_ids_ingestion(),
        'items': items_ingestion(),
        'lobby_type': lobby_type_ingestion()
    }

fact_tables_mapping = {
    'publicmatches'
}