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
    #call logger function, source: function name
    list_to_str = logger(f"{convert_list_to_string_df.__name__} function started", f"{convert_list_to_string_df.__name__}")
    list_to_str.new_or_existing_run()

    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].astype(str)

    list_to_str = logger(f"{convert_list_to_string_df.__name__} function finished", f"{convert_list_to_string_df.__name__}")
    list_to_str.new_or_existing_run()
    
    return df


def prepare_schema_for_df(table_name):
    #call logger function, source: function name
    prep_schema = logger(f"{prepare_schema_for_df.__name__} function started", f"{prepare_schema_for_df.__name__}")
    prep_schema.new_or_existing_run()
    schemas = open_schemas()
    data_table = [table for table in schemas.get("tables", []) if table.get("name") == table_name]

    columns = data_table[0]['columns']

    schema = []
    for col in columns:
        row = [col['name'], col['type'], col['constraints']]
        schema.append(row)

    schema_str = ', '.join(' '.join(column) for column in schema)

    prep_schema2 = logger(f"{prepare_schema_for_df.__name__} function finished", f"{prepare_schema_for_df.__name__}")
    prep_schema2.new_or_existing_run()
    return schema_str

class logger():
    # each run should start with determining whether this is the very beginning of a new run, or a continuation of an existing run. 
    # in theory 1 load per day should run. Hence, version number should be the date of the run itself.
        # if more runs are run, there is a _x identifier that helps identifying the latest run.
    # check if log file exists
    # if exists, get its ID, create new with ID incremented by 1
    # function's return result should be write_status_log(result) or write_status_log(exception)
    #2nd stage: add deleted, updated, inserted records as well - this will lead to versioning, disaster recovery etc later.
    def __init__(self, message, function_name) -> None:
        self.message = message
        self.function_name = function_name
        self.timestamp = datetime.now()
        self.today = datetime.today().strftime("%Y-%m-%d")
        self.log_directory = "logs"

    def new_or_existing_run(self):
        self.found_files = os.listdir("logs")

        if any("ongoing" in file for file in self.found_files):
            for f in self.found_files:
                if "ongoing" in f:
                    self.ongoing_file = f
                    break
            return self.write_status_log()
        else:
            return self.create_new_log()


    def create_new_log(self):
        #file does not exist, process is just starting, creating new log file.
        #   creating file name => date_version_ongoing.json
        #       checking if there is already a file with today's date, and getting the latest.
                #this scenario can happen, as on given day multiple manually triggered runs can take place, so we need to check the current version and increment by 1 to create the new version
        
        files = [0]

        #check directory and collect file names that contain today's date
        for file in self.found_files:
            # if file with today's date is found in dir, add its version number to a list
                if self.today in file:
                    todays_file_version = int(file.replace(".txt","")[11:])
                    files.append(todays_file_version) 
        
        
        self.new_version = max(files)+1
        self.ongoing_file = f"{self.today}_{self.new_version}_ongoing.txt"

        #create new json with filename
        try:
            # Open the file in write mode ('w')
            with open(f"logs/{self.ongoing_file}", "w") as text_file:
                pass
            # Output a message indicating the file creation
            return self.write_status_log()
        except Exception as e:
            print(f"Error occurred while creating the file: {e}")      
        

    def write_status_log(self):
        # write content
        content = f"{datetime.now()} : function {self.function_name}, message: {self.message} \n"
    
        # open file that contains "ongoing" in its title
        with open(f"logs/{self.ongoing_file}", "a") as text_file:
            # delete ]
            text_file.write(content)
        
        return f"content added to json"

    def rename_log_file(self):
        #this function is only called at the very end of the pipeline once all functions and methods were run.
        #rename log_file from date_versionnumber_ongoing.json to date_versionnumber.json
        self.new_file_name = self.ongoing_file.replace("_ongoing","")
        os.rename(f"logs/{self.ongoing_file}", f"logs/{self.new_file_name}")


        # delete any files with "ongoing" string in its name
        return "rename_log_file methodrenaming log file in progress"
    
