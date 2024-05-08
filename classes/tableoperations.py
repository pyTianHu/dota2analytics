import sqlite3
import pandas as pd
from utils.utils import logger, selected_columns

class TableOperations:
    def __init__(self, db_name, table_name, schema = None, data = None) -> None:
        self.db_name = db_name
        self.table_name = table_name
        self.schema = schema
        self.data = data
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
    

    def check_if_table_exists(self):
        table_exists = logger(f"{TableOperations.check_if_table_exists.__name__} method started, table to check: {self.table_name}, database:{self.db_name}", f"{TableOperations.check_if_table_exists.__name__}")
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
            table_exists = logger(f"Table {self.table_name} exists in {self.db_name}", f"{TableOperations.check_if_table_exists.__name__}")
            table_exists.new_or_existing_run()
            return True
        else:
            table_exists = logger(f"Table {self.table_name} does not exist in {self.db_name}", f"{TableOperations.check_if_table_exists.__name__}")
            table_exists.new_or_existing_run()
            return False
        

    def create_table(self):
        ct = logger(f"{TableOperations.create_table.__name__} method started, table to create: {self.table_name}, database:{self.db_name}", f"{TableOperations.create_table.__name__}")
        ct.new_or_existing_run()
        query = f'''
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                {self.schema}
                )
                '''
        try:
            self.cursor.execute(query)
            self.conn.commit()
            ct2 = logger(f"{TableOperations.create_table.__name__} method finished, {self.table_name} is hereby created in database:{self.db_name}", f"{TableOperations.create_table.__name__}")
            ct2.new_or_existing_run()
        except Exception as e:
            ct2 = logger(f"{TableOperations.create_table.__name__} method finished, {self.table_name} has not been created in database:{self.db_name} due to exception: {e}", f"{TableOperations.create_table.__name__}")
            ct2.new_or_existing_run()
            

    def add_new_column(self):
        ct = logger(f"{TableOperations.add_new_column.__name__} method started, table to alter: {self.table_name}, database:{self.db_name}", f"{TableOperations.add_new_column.__name__}")
        ct.new_or_existing_run()


        ct2 = logger(f"Column {self.column_name} is hereby added to {self.table_name}, database:{self.db_name}", f"{TableOperations.add_new_column.__name__}")
        ct2.new_or_existing_run()

        ct2 = logger(f"Column {self.column_name} is not added to {self.table_name} due to exception: {e}, database:{self.db_name}", f"{TableOperations.add_new_column.__name__}")
        ct2.new_or_existing_run()
        pass


    def remove_existing_column(self):
        ct = logger(f"{TableOperations.remove_existing_column.__name__} method started, table to alter: {self.table_name}, database:{self.db_name}", f"{TableOperations.remove_existing_column.__name__}")
        ct.new_or_existing_run()


        ct2 = logger(f"Column {self.column_name} is hereby removed from {self.table_name}, database:{self.db_name}", f"{TableOperations.remove_existing_column.__name__}")
        ct2.new_or_existing_run()

        ct2 = logger(f"Column {self.column_name} is not removed from {self.table_name} due to exception: {e}, database:{self.db_name}", f"{TableOperations.remove_existing_column.__name__}")
        ct2.new_or_existing_run()
        pass


    def drop_table(self):
        ct = logger(f"{TableOperations.drop_table.__name__} method started, table to drop: {self.table_name}, database:{self.db_name}", f"{TableOperations.drop_table.__name__}")
        ct.new_or_existing_run()
        query = f'''
                DROP TABLE {self.table_name}
                '''
        try:
            self.cursor.execute(query)
            self.conn.commit()
            ct = logger(f"Table {self.table_name} is hereby dropped, database:{self.db_name}", f"{TableOperations.drop_table.__name__}")
            ct.new_or_existing_run()
        except Exception as e:
            ct = logger(f"Table {self.table_name} has not been dropped due to exception: {e}, database:{self.db_name}", f"{TableOperations.drop_table.__name__}")
            ct.new_or_existing_run()


    def insert_df_into_table(self):
        ct = logger(f"{TableOperations.insert_df_into_table.__name__} method started, table to delete all from: {self.table_name}, database:{self.db_name}", f"{TableOperations.insert_df_into_table.__name__}")
        ct.new_or_existing_run()

        try:
            self.data.to_sql(self.table_name, self.conn, if_exists='replace', index=False)
            ct2 = logger(f"Data was inserted into {self.table_name}, database:{self.db_name}", f"{TableOperations.insert_df_into_table.__name__}")
            ct2.new_or_existing_run()
            return True
        except Exception as e:
            ct2 = logger(f"Rows could not be inserted into {self.table_name} due to exception: {e}, database:{self.db_name}", f"{TableOperations.insert_df_into_table.__name__}")
            ct2.new_or_existing_run()
            return e


    def delete_all_from_table(self):
        ct = logger(f"{TableOperations.delete_all_from_table.__name__} method started, table to delete all from: {self.table_name}, database:{self.db_name}", f"{TableOperations.delete_all_from_table.__name__}")
        ct.new_or_existing_run()


        ct2 = logger(f"All rows have been deleted from {self.table_name}, database:{self.db_name}", f"{TableOperations.delete_all_from_table.__name__}")
        ct2.new_or_existing_run()

        ct2 = logger(f"Rows could not be deleted from {self.table_name} due to exception: {e}, database:{self.db_name}", f"{TableOperations.delete_all_from_table.__name__}")
        ct2.new_or_existing_run()
        pass


    def delete_from_table_where(self):
        ct = logger(f"{TableOperations.delete_from_table_where.__name__} method started, table to alter: {self.table_name}, database:{self.db_name}", f"{TableOperations.delete_from_table_where.__name__}")
        ct.new_or_existing_run()


        ct2 = logger(f"Record {self.record_to_drop} has been deleted from {self.table_name}, database:{self.db_name}", f"{TableOperations.delete_from_table_where.__name__}")
        ct2.new_or_existing_run()

        ct2 = logger(f"Record {self.record_to_drop} could not be deleted from {self.table_name} due to exception: {e}, database:{self.db_name}", f"{TableOperations.delete_from_table_where.__name__}")
        ct2.new_or_existing_run()
        pass


    def select_all_to_df(self):
        satdf = logger(f"{TableOperations.select_all_to_df.__name__} method started, table to select all from and insert into a pandas dataframe: {self.table_name}, database: {self.db_name}", f"{TableOperations.select_all_to_df}")
        satdf.new_or_existing_run()
        query = f'''
                SELECT *
                FROM {self.table_name}
                '''
        try:
            df = pd.read_sql_query(query, self.conn)
            ct2 = logger(f"{TableOperations.select_all_to_df.__name__} method finished, {self.table_name} is hereby created as a dataframe", f"{TableOperations.select_all_to_df.__name__}")
            ct2.new_or_existing_run()
            return df
        except Exception as e:
            ct2 = logger(f"{TableOperations.select_all_to_df.__name__} method finished, {self.table_name} has not been created as a dataframe due to exception: {e}", f"{TableOperations.select_all_to_df.__name__}")
            ct2.new_or_existing_run()


    def select_cols_to_df(self):
        sctdf = logger(f"{TableOperations.select_cols_to_df.__name__} method started, table to select all from and insert into a pandas dataframe: {self.table_name}, database: {self.db_name}", f"{TableOperations.select_cols_to_df}")
        sctdf.new_or_existing_run()
        
        cols = selected_columns.get(self.table_name)
        coluns = ', '.join(cols)

        query = f'''
                SELECT {coluns}
                FROM {self.table_name}
                '''
        try:
            df = pd.read_sql_query(query, self.conn)
            ct2 = logger(f"{TableOperations.select_all_to_df.__name__} method finished, {self.table_name} is hereby created as a dataframe", f"{TableOperations.select_cols_to_df.__name__}")
            ct2.new_or_existing_run()
            return df
        except Exception as e:
            ct2 = logger(f"{TableOperations.select_all_to_df.__name__} method finished, {self.table_name} has not been created as a dataframe due to exception: {e}", f"{TableOperations.select_cols_to_df.__name__}")
            ct2.new_or_existing_run()        


    def select_sample_from_table(self):
        ct = logger(f"{TableOperations.select_sample_from_table.__name__} method started, table to query: {self.table_name}, database:{self.db_name}", f"{TableOperations.select_sample_from_table.__name__}")
        ct.new_or_existing_run()


        ct2 = logger(f"Sample data for {self.table_name}: {self.sample}, database:{self.db_name}", f"{TableOperations.select_sample_from_table.__name__}")
        ct2.new_or_existing_run()

        ct2 = logger(f"Sample data for {self.table_name} cannot be provided due to exception: {e}, database:{self.db_name}", f"{TableOperations.select_sample_from_table.__name__}")
        ct2.new_or_existing_run()
        pass


    def print_schema(self):
        ct = logger(f"{TableOperations.print_schema.__name__} method started for table {self.table_name}, database:{self.db_name}", f"{TableOperations.print_schema.__name__}")
        ct.new_or_existing_run()


        ct2 = logger(f"{self.table_name}'s schema: {self.schema}, database:{self.db_name}", f"{TableOperations.print_schema.__name__}")
        ct2.new_or_existing_run()

        ct2 = logger(f"Cannot print schema for {self.table_name} due to exception: {e}, database:{self.db_name}", f"{TableOperations.print_schema.__name__}")
        ct2.new_or_existing_run()
        pass


