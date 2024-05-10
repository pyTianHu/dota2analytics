import sys
sys.path.append('../dota2')

selected_columns = {
    'heroes': ["id", "localized_name"],
    #'herostats': [],
    'publicmatches': ["match_id", "start_time", "radiant_win", "lobby_type", "game_mode", "radiant_team", "dire_team"],
    #'abilities': [],
    #'ability_ids': [],
    'game_mode': ["id", "name"],
    #'hero_abilities': [],
    #'items_ids': [],
    #'items': [],
    'patch': ["name", "date", "id"],
    'lobby_type': ["id", "name"]
}

#criteria on what rows to filter and what to exclude => values must be in format of a list, even if it is just one value
rows_isin = {
    'game_mode': {
        'id':{
            'select':(1,2)
        }
    },
    'patch': {
        'id':{
            'drop':(range(0,54))
        }
    },
    'lobby_type': {
        'id':{
            'select':(0,5,6,7)
        }
    }
}
