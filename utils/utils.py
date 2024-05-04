import json
import sys
import os
from datetime import datetime

sys.path.append('../dota2')
from scripts.data_ingestion import *

def open_schemas():
    with open('utils\schemas.json', "r") as json_file:
        data = json.load(json_file)

        return data


def convert_list_to_string_df(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].astype(str)

    return df


def prepare_schema_for_df(table_name):
    schemas = open_schemas()
    data_table = [table for table in schemas.get("tables", []) if table.get("name") == table_name]

    columns = data_table[0]['columns']

    schema = []
    for col in columns:
        row = [col['name'], col['type'], col['constraints']]
        schema.append(row)

    schema_str = ', '.join(' '.join(column) for column in schema)

    return schema_str

class logger():
    # each run should start with determining whether this is the very beginning of a new run, or a continuation of an existing run. 
    # in theory 1 load per day should run. Hence, version number should be the date of the run itself.
        # if more runs are run, there is a _x identifier that helps identifying the latest run.
    # check if log file exists
    # if exists, get its ID, create new with ID incremented by 1
    # function's return result should be write_status_log(result) or write_status_log(exception)
    #2nd stage: add deleted, updated, inserted records as well - this will lead to versioning, disaster recovery etc later.
    def __init__(self, message) -> None:
        self.message = message
        self.timestamp = datetime.now()
        self.today = datetime.today().strftime("%Y-%m-%d")
        self.log_directory = "logs"

    def new_or_existing_run(self):
        #determines whether the currently called run is already ongoing or a new one
        # this is based on file name - if there is a file name with "ongoing" in its name, we open that and continue populating it.
        # if no file with such name, create a new log file

        #list of file names
        self.found_files = os.listdir("logs")

        #check for "ongoing" string in any elements of the list, if not found, create
        #for file in self.found_files:
        if any("ongoing" in file for file in self.found_files):
            #self.ongoing_file = file
            print("new_or_existing_run method => ongoing term is found in one of the files in the folder, passing to write_status_log function")
            return self.write_status_log()
        else:
            print("new_or_existing_run method => ongoing term is not found in any of the files in the directory, calling create_new_log function")
            return self.create_new_log()


    def create_new_log(self):
        #file does not exist, process is just starting, creating new log file.
        #   creating file name => date_version_ongoing.json
        #       checking if there is already a file with today's date, and getting the latest.
                #this scenario can happen, as on given day multiple manually triggered runs can take place, so we need to check the current version and increment by 1 to create the new version
        
        files = []

        for file in self.found_files:
            
            
            if any(self.today in file for file in self.found_files):
                todays_file_version = int(file.replace(".json","")[11:])
                files.append(todays_file_version)            
                
            else:
                return "file with today's date is not found in logs folder"
        
        self.new_version = max(files)+1
        self.ongoing_file = f"{self.today}_{self.new_version}_ongoing.json"
        
        return "create_new_log method,", self.write_status_log()
       
        

    def write_status_log(self):
        
        return f"write_status_log => writing status log is in progress. file name: TBA", self.rename_log_file()

    def rename_log_file(self):
        #rename log_file from date_versionnumber_ongoing.json to date_versionnumber.json
        return "rename_log_file methodrenaming log file in progress"
    


table_function_mapping = {
        'heroes': heroes_ingestion(),
        'herostats': herostats_ingestion(),
        'publicmatches': publicmatches_ingestion(),
        'abilities': abilities_ingestion(),
        'ability_ids': ability_ids_ingestion(),
        'game_mode': game_mode_ingestion(),
        'hero_abilities': hero_abilities_ingestion(),
        'items_ids': items_ids_ingestion(),
        'items': items_ingestion(),
        'patch': patch_ingestion(),
        'lobby_type': lobby_type_ingestion()
    }
    


selected_columns = {
    'heroes': [],
    'herostats': [],
    'publicmatches': [],
    'abilities': [],
    'ability_ids': [],
    'game_mode': [],
    'hero_abilities': [],
    'item_ids': [],
    'items': [],
    'patch': [],
    'lobby_type': []
}