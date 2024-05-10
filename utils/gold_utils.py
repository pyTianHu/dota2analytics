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

column_rename = {
    'dim_heroes':{
        'id': 'heroes_id',
        'localized_name': 'hero_name'
    },
    'f_pubs':{
        'match_id': 'match_id',
        'start_time': 'game_start_timestamp',
        'radiant_win': 'winning_team', #transformation should include => if radiant_win = 1 then "Radiant" else "Dire"
        'lobby_type': 'lobby_type_id',
        'game_mode': 'game_mode_id',
        'radiant_team': 'radiant_team_heroes',
        'dire_team': 'dire_team_heroes'
    },
    'f_game_modes': {
        'id': 'game_modes_id',
        'name': 'game_mode_name'
    },
    'f_patches': {
        'name': 'patch_name',
        'date': 'patch_release_date',
        'id': 'patch_id'
    },
    'f_lobby_types': {
        'id': 'lobby_type_id',
        'name': 'lobby_type_name'
    }
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