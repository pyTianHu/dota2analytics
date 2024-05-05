import sqlite3
import pandas as pd
from utils.utils import logger

class TableOperations:
    def __init__(self, db_name, table_name, schema = None, data = None) -> None:
        self.db_name = db_name
        self.table_name = table_name
        self.schema = schema
        self.data = data
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
    

    def check_if_table_exists(self):
        table_exists = logger(f"tableoperations.check_if_table_exists method started, table to check: {self.table_name}, database:{self.db_name}", "check_if_table_exists")
        table_exists.new_or_existing_run()
        query = f'''
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' 
                AND name="{self.table_name}"
                '''
        self.cursor.execute(query)
        result = self.cursor.fetchone()

        if result:
            #call logger function w result
            table_exists = logger(f"Table {self.table_name} exists in {self.db_name}", "check_if_table_exists")
            table_exists.new_or_existing_run()
            return f"Table {self.table_name} exists in {self.db_name}"
        else:
            #call logger function w result
            table_exists = logger(f"Table {self.table_name} does not exist in {self.db_name}", "check_if_table_exists")
            table_exists.new_or_existing_run()
            return f"Table {self.table_name} does not exist in {self.db_name}"
        

    def create_table(self):
        ct = logger(f"tableoperation.create_table method started, table to create: {self.table_name}, database:{self.db_name}", "create_table")
        ct.new_or_existing_run
        query = f'''
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                {self.schema}
                )
                '''
        try:
            self.cursor.execute(query)
            self.conn.commit()
            #call logger function w result
            return f"Table {self.db_name}.{self.table_name} is hereby created"
        except Exception as e:
            #call logger function w exceptiont
            return f"Table is not created {e}"
    

    def add_new_column(self):
        ct = logger(f"tableoperation.add_new_column method started, table to alter: {self.table_name}, database:{self.db_name}", "add_new_column")
        ct.new_or_existing_run


        ct2 = logger(f"Column {self.column_name} is hereby added to {self.table_name}, database:{self.db_name}", "add_new_column")
        ct2.new_or_existing_run

        ct2 = logger(f"Column {self.column_name} is not added to {self.table_name} due to exception: {e}, database:{self.db_name}", "add_new_column")
        ct2.new_or_existing_run
        pass


    def remove_existing_column(self):
        ct = logger(f"tableoperation.remove_existing_column method started, table to alter: {self.table_name}, database:{self.db_name}", "remove_existing_column")
        ct.new_or_existing_run


        ct2 = logger(f"Column {self.column_name} is hereby removed from {self.table_name}, database:{self.db_name}", "remove_existing_column")
        ct2.new_or_existing_run

        ct2 = logger(f"Column {self.column_name} is not removed from {self.table_name} due to exception: {e}, database:{self.db_name}", "remove_existing_column")
        ct2.new_or_existing_run
        pass


    def drop_table(self):
        ct = logger(f"tableoperation.drop_table method started, table to drop: {self.table_name}, database:{self.db_name}", "drop_table")
        ct.new_or_existing_run
        query = f'''
                DROP TABLE {self.table_name}
                '''
        try:
            self.cursor.execute(query)
            self.conn.commit()
            #call logger function w result
            ct = logger(f"Table {self.table_name} is hereby dropped, database:{self.db_name}", "drop_table")
            ct.new_or_existing_run
            #return "Table is hereby dropped"
        except Exception as e:
            #call logger function w exception
            ct = logger(f"Table {self.table_name} has not been dropped due to exception: {e}, database:{self.db_name}", "drop_table")
            ct.new_or_existing_run
            #return f"Table is not dropped {e}"


    def insert_df_into_table(self):
        try:
            self.data.to_sql(self.table_name, self.conn, if_exists='replace', index=False)
            #call logger function w result
            return "Data was inserted"
        except Exception as e:
            #call logger function w exception
            return f"No data was inserted: {e}"


    def delete_all_from_table(self):
        pass


    def delete_from_table_where(self):
        pass


    def select_sample_from_table(self):
        pass


    def print_schema(self):
        pass


