import sys
sys.path.append('../dota2')

from classes.tableoperations import TableOperations
from data_ingestion import heroes_ingestion
import sqlite3
from utils.utils import open_schemas

#get column names from schemas.json, pass it as schema into table_creation function, create table here by calling the method
def heroes_creation(db_name, table_name):
    schemas = open_schemas()
    heroes_table = [table for table in schemas.get("tables", []) if table.get("name") == table_name]

    columns = heroes_table[0]['columns']

    schema = []
    for col in columns:
        row = [col['name'], col['type'], col['constraints']]
        schema.append(row)

    schema_str = ', '.join(' '.join(column) for column in schema)

    heroes = TableOperations(db_name, table_name, schema_str)
    print(heroes.create_table())

    heroes_df = heroes_ingestion()

    conn = sqlite3.connect(db_name)

    try:
        heroes_df.to_sql(table_name, conn, if_exists='replace', index=False)
        return "Data was inserted"
    except Exception as e:
        return f"No data was inserted: {e}"


heroes_df = heroes_creation('dot_raw_prod.db','heroes')