import sys
sys.path.append('../dota2')

selected_columns = {
    'patch': ["name", "date", "id"],
    'heroes': ["id", "localized_name"],
    'herostats': [],
    'publicmatches': ["match_id", "start_time", "radiant_win", "lobby_type", "game_mode", "radiant_team", "dire_team"],
    'abilities': [],
    'ability_ids': [],
    'game_mode': ["id", "name"],
    'hero_abilities': [],
    'items_ids': [],
    'items': [],
    'lobby_type': ["id", "name"]
}