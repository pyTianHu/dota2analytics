import pandas as pd
import requests

def response_to_df(url):
    response = requests.get(url, verify=False)
    df = pd.DataFrame(response.json())
    return df


def transponse_if_needed(df):
    #transposing dataframes where the column&row structure seem to be messed up
    if len(df) < len(df.columns):
        df = df.transpose()
    else:
        pass
    return df


def heroes_ingestion():
    url = "https://api.opendota.com/api/heroes" 
    data = response_to_df(url)
    df = transponse_if_needed(data)
    return df


def herostats_ingestion():
    url = "https://api.opendota.com/api/heroStats"
    data = response_to_df(url)
    df = transponse_if_needed(data)
    return df


def publicmatches_ingestion():
    url = "https://api.opendota.com/api/publicMatches"
    data = response_to_df(url)
    df = transponse_if_needed(data)
    return df

def abilities_ingestion():
    url = "https://api.opendota.com/api/constants/abilities"
    data = response_to_df(url)
    df = transponse_if_needed(data)
    return df

def ability_ids_ingestion():
    url = "https://api.opendota.com/api/constants/ability_ids"
    # specifying index here due to only having 1 row
    response = requests.get(url, verify=False)
    data = pd.DataFrame(response.json(), index=['row1'])
    df = transponse_if_needed(data)
    return df

def game_mode_ingestion():
    # needs to be transposed - 3row, 26 col => must be transformed before raw layer
    url = "https://api.opendota.com/api/constants/game_mode"
    data = response_to_df(url)
    df = transponse_if_needed(data)
    return df


def hero_abilities_ingestion():
    url = "https://api.opendota.com/api/constants/hero_abilities"
    data = response_to_df(url)
    df = transponse_if_needed(data)
    return df


def item_ids_ingestion():
    url = "https://api.opendota.com/api/constants/item_ids"
    response = requests.get(url, verify=False)
    data = pd.DataFrame(response.json(), index=['row1'])
    df = transponse_if_needed(data)
    return df


def items_ingestion():
    url = "https://api.opendota.com/api/constants/items"
    data = response_to_df(url)
    df = transponse_if_needed(data)
    return df


def patch_ingestion():
    url = "https://api.opendota.com/api/constants/patch"
    data = response_to_df(url)
    df = transponse_if_needed(data)
    return df


def lobby_type_ingestion():
    url = "https://api.opendota.com/api/constants/lobby_type"
    df = response_to_df(url)
    data = transponse_if_needed(data)
    return data


print(item_ids_ingestion())