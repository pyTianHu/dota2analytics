import json

def open_schemas():
    with open('utils\schemas.json', "r") as json_file:
        data = json.load(json_file)

        return data