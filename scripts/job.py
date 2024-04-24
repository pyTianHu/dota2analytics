import sys
sys.path.append('../dota2')

from classes.tableoperations import TableOperations
from data_ingestion import heroes_ingestion
import sqlite3
from utils.utils import open_schemas

#get column names from schemas.json, pass it as schema into table_creation function, create table here by calling the method
def heroes_creation(db_name, table_name):
    
    #Get data
    heroes_df = heroes_ingestion()
    
    #Prep schema
    schemas = open_schemas()
    heroes_table = [table for table in schemas.get("tables", []) if table.get("name") == table_name]

    columns = heroes_table[0]['columns']

    schema = []
    for col in columns:
        row = [col['name'], col['type'], col['constraints']]
        schema.append(row)

    schema_str = ', '.join(' '.join(column) for column in schema)

    #instantiate object
    heroes = TableOperations(db_name, table_name, schema_str, heroes_df)
    print(heroes.create_table())

    #conn = sqlite3.connect(db_name)

    #execute insert
    try:
        print(heroes.insert_df_into_table())
        return "Data was inserted"
    except Exception as e:
        return print(f"No data was inserted: {e}")


heroes_df = heroes_creation('dot_raw_prod.db','heroes')