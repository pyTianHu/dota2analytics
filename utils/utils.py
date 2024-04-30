import json
import sys

sys.path.append('../dota2')
from scripts.data_ingestion import *

def open_schemas():
    with open('utils\schemas.json', "r") as json_file:
        data = json.load(json_file)

        return data
    

#print(open_schemas())

def convert_list_to_string_df(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].astype(str)

    return df

def prepare_schema_for_df(table_name):
    schemas = open_schemas()
    data_table = [table for table in schemas.get("tables", []) if table.get("name") == table_name]

    columns = data_table[0]['columns']

    schema = []
    for col in columns:
        row = [col['name'], col['type'], col['constraints']]
        schema.append(row)

    schema_str = ', '.join(' '.join(column) for column in schema)

    return schema_str

def write_status_log():
    # check if log file exists
    # if exists, open, if not, create
    # function's return result should be write_status_log(result) or write_status_log(exception)
    pass


table_function_mapping = {
        'heroes': heroes_ingestion(),
        'herostats': herostats_ingestion(),
        'publicmatches': publicmatches_ingestion(),
        'abilities': abilities_ingestion(),
        'ability_ids': ability_ids_ingestion(),
        'game_mode': game_mode_ingestion(),
        'hero_abilities': hero_abilities_ingestion(),
        'items_ids': items_ids_ingestion(),
        'items': items_ingestion(),
        'patch': patch_ingestion(),
        'lobby_type': lobby_type_ingestion()
    }
    


selected_columns = {
    'heroes': [],
    'herostats': [],
    'publicmatches': [],
    'abilities': [],
    'ability_ids': [],
    'game_mode': [],
    'hero_abilities': [],
    'item_ids': [],
    'items': [],
    'patch': [],
    'lobby_type': []
}