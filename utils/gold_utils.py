import sys
sys.path.append('../dota2')

selected_columns = {
    'patch': ["name", "date", "id"],
    'heroes': ["id", "localized_name"],
    #'herostats': [],
    'publicmatches': ["match_id", "start_time", "radiant_win", "lobby_type", "game_mode", "radiant_team", "dire_team"],
    #'abilities': [],
    #'ability_ids': [],
    'game_mode': ["id", "name"],
    #'hero_abilities': [],
    #'items_ids': [],
    #'items': [],
    'lobby_type': ["id", "name"]
}



table_rename = {
    'patch': 'dim_patches',
    'heroes': 'dim_heroes',
    #'herostats': [],
    'publicmatches': 'f_pubs',
    #'abilities': [],
    #'ability_ids': [],
    'game_mode': 'dim_game_modes',
    #'hero_abilities': [],
    #'items_ids': [],
    #'items': [],
    'lobby_type': 'dim_lobby_types'
}

column_rename = {
    'dim_patches': {
        'name': 'patch_name',
        'date': 'patch_release_date',
        'id': 'patch_id'
    },
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
    'dim_game_modes': {
        'id': 'game_modes_id',
        'name': 'game_mode_name'
    },
    'dim_lobby_types': {
        'id': 'lobby_type_id',
        'name': 'lobby_type_name'
    }
}


table_constraints = {
    'dim_patches': {
        "patch_name": ("STRING", "NOT NULL"),
        "patch_release_date": ("DATETIME", "NOT NULL"),
        "patch_id": ("INTEGER", "PRIMARY KEY")
    },
    'dim_heroes': {
        "heroes_id": ("INTEGER", "PRIMARY KEY"),
        "hero_name": ("STRING", "NOT NULL")
    },
    'f_pubs' : {
        "match_id": ("INTEGER", "PRIMARY KEY"), 
        "game_start_timestamp": ("TIMESTAMP", "NOT NULL"), 
        "winning_team": ("INTEGER", "NOT NULL"), 
        "lobby_type_id": ("INTEGER", "NOT NULL"), 
        "game_mode_id": ("INTEGER", "NOT NULL"), 
        "radiant_team_heroes": ("STRING", "NOT NULL"), 
        "dire_team_heroes": ("STRING", "NOT NULL")
    },
    'dim_game_modes' :{
        "game_modes_id": ("INTEGER", "PRIMARY KEY"),
        "game_modes_name": ("STRING", "NOT NULL")
    },
    'dim_lobby_types': {
        "lobby_type_id": ("INTEGER", "PRIMARY KEY"),
        "lobby_type_name": ("STRING", "NOT NULL")
    }
}