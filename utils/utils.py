import json
from scripts.data_ingestion import heroes_ingestion, herostats_ingestion

def open_schemas():
    with open('utils\schemas.json', "r") as json_file:
        data = json.load(json_file)

        return data

def convert_list_to_string_df(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].astype(str)

    return df

table_function_mapping = {
        'heroes': heroes_ingestion(),
        'herostats': herostats_ingestion()
    }