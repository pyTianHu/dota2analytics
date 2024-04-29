import sys
sys.path.append('../dota2')

from classes.tableoperations import TableOperations
from data_ingestion import heroes_ingestion, herostats_ingestion
import sqlite3
from utils.utils import open_schemas

   
def table_create_and_ingest(db_name, table_name):
    
    #mapping
    table_function_mapping = {
        'heroes': heroes_ingestion(),
        'herostats': herostats_ingestion()
    }
    
    #calling and executing ingestion function, storing it in df variable
    df = table_function_mapping.get(table_name)

    print(df.info())
    print(df)

    # Data transformation
    # lists into strings
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].astype(str)
    

    #Prep schema
    schemas = open_schemas()
    data_table = [table for table in schemas.get("tables", []) if table.get("name") == table_name]

    columns = data_table[0]['columns']

    schema = []
    for col in columns:
        row = [col['name'], col['type'], col['constraints']]
        schema.append(row)

    schema_str = ', '.join(' '.join(column) for column in schema)

    #instantiate object
    ingested_table = TableOperations(db_name, table_name, schema_str, df)

    #check whether table already exists
    exists = ingested_table.check_if_table_exists()
    if exists:
        print(exists)
        pass
    else:
        #if table does not exist, create
        print(exists)
        print(ingested_table.create_table())

    #execute insert
    # return has to be edited, as it returns invalid result. function runs successfully and that is what it returns.
    try:
        print(ingested_table.insert_df_into_table())
        return print("Data was inserted")
    except Exception as e:
        return print(f"No data was inserted: {e}")


table_create_and_ingest('dot_dev.db', 'heroes')

  