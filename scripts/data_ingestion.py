import pandas as pd
import requests


def heroes_ingestion():
    heroes_url = "https://api.opendota.com/api/heroes"
    response = requests.get(heroes_url, verify=False)
    df = pd.DataFrame(response.json())
    #df['roles'] = df['roles'].apply(lambda x: ','.join(x))

    return df


def herostats_ingestion():
    herostats_url = "https://api.opendota.com/api/heroStats"
    response = requests.get(herostats_url, verify=False)
    df = pd.DataFrame(response.json())
    
    return df


def publicmatches_ingestion():
    publicmatches_url = "https://api.opendota.com/api/publicMatches"
    response = requests.get(publicmatches_url, verify=False)
    df = pd.DataFrame(response.json())

    return df