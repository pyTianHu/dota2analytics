import sqlite3
import pandas as pd
from utils.utils import logger
from utils.bronze_utils import selected_columns as bronze_selected_columns
from utils.gold_utils import table_constraints, table_rename, selected_columns as gold_selected_columns

class TableOperations:
    def __init__(self, db_name, table_name, schema = None, data = None) -> None:
        self.db_name = db_name
        self.table_name = table_name
        self.schema = schema
        self.data = data
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
    

    def check_if_table_exists(self):
        query = f'''
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' 
                AND name="{self.table_name}"
                '''
        self.cursor.execute(query)
        result = self.cursor.fetchone()

        if result:
            return True
        else:
            return False
        

    def create_table(self):
        query = f'''
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                {self.schema}
                )
                '''
        try:
            self.cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            return e
            

    def drop_table(self):
        query = f'''
                DROP TABLE {self.table_name}
                '''
        try:
            self.cursor.execute(query)
            self.conn.commit()
        except Exception as e:
            e


    def insert_df_into_table(self):
        try:
            self.data.to_sql(self.table_name, self.conn, if_exists='replace', index=False)
            return True
        except Exception as e:
            return e


    def select_all_to_df(self):
        query = f'''
                SELECT *
                FROM {self.table_name}
                '''
        try:
            df = pd.read_sql_query(query, self.conn)
            return df
        except Exception as e:
            return e


    def select_cols_to_df(self, layer):
        if layer == "gold":
            cols = gold_selected_columns.get(self.table_name)
        elif layer == "bronze":
            cols = bronze_selected_columns.get(self.table_name)
        
        coluns = ', '.join(cols)

        query = f'''
                SELECT {coluns}
                FROM {self.table_name}
                '''
        try:
            df = pd.read_sql_query(query, self.conn)
            return df
        except Exception as e:
            return e      


    def select_sample_from_table(self):
        pass


    def print_schema(self):
        pass


    def add_new_column(self):
        pass


    def remove_existing_column(self):
        pass


    def delete_all_from_table(self):
        pass


    def delete_from_table_where(self):
        pass


class ManageTable():
    def __init__(self, db_name, table_name, schema_str = None, data_df = None):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.db_name = db_name
        self.table_name = table_name

    
