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

table_rename = {
    'heroes': 'dim_heroes',
    #'herostats': [],
    'publicmatches': 'f_pubs',
    #'abilities': [],
    #'ability_ids': [],
    'game_mode': 'f_game_modes',
    #'hero_abilities': [],
    #'items_ids': [],
    #'items': [],
    'patch': 'f_patches',
    'lobby_type': 'f_lobby_types'
}


table_constraints = {
    'dim_heroes': {
        "id": ("INTEGER, PRIMARY KEY"),
        "localized_name": ("STRING, NOT NULL")
    },
    'f_pubs' : {
        "match_id": ("INTEGER, PRIMARY KEY"), 
        "start_time": ("TIMESTAMP NOT NULL"), 
        "radiant_win": ("INTEGER NOT NULL"), 
        "lobby_type": ("INTEGER NOT NULL"), 
        "game_mode": ("INTEGER NOT NULL"), 
        "radiant_team": ("STRING NOT NULL"), 
        "dire_team": ("STRING NOT NULL")
    },
    'f_game_modes' :{
        "id": ("INTEGER PRIMARY KEY"),
        "name": ("STRING NOT NULL")
    },
    'f_patches': {
        "name": ("STRING NOT NULL"),
        "date": ("DATETIME NOT NULL"),
        "id": ("INTEGER PRIMARY KEY")
    },
    'f_lobby_types': {
        "id": ("INTEGER PRIMARY KEY"),
        "name": ("STRING NOT NULL")
    }
}