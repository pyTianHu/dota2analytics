import json
from scripts.data_ingestion import heroes_ingestion, herostats_ingestion

def open_schemas():
    with open('utils\schemas.json', "r") as json_file:
        data = json.load(json_file)

        return data

table_function_mapping = {
        'heroes': heroes_ingestion(),
        'herostats': herostats_ingestion()
    }