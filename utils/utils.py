import json

def open_schemas():
    with open('utils/schemas.json', 'r') as file:
        data = json.load(file)

    return data