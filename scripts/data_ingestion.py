import pandas as pd
import requests


def heroes_ingestion():
    heroes_url = "https://api.opendota.com/api/heroes"
    response = requests.get(heroes_url, verify=False)
    df = pd.DataFrame(response.json())
    #df['roles'] = df['roles'].apply(lambda x: ','.join(x))

    return df

def herostats_ingestion():
    heroes_url = "https://api.opendota.com/api/heroStats"
    response = requests.get(heroes_url, verify=False)
    df = pd.DataFrame(response.json())
    
    return df
