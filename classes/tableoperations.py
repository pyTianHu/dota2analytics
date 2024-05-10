import sqlite3
import pandas as pd
from utils.utils import logger
from utils.bronze_utils import bronze_selected_columns
from utils.silver_utils import silver_selected_columns

class TableOperations:
    def __init__(self, source_db_name = None, target_db_name = None, source_table_name = None, target_table_name = None, schema = None, data = None) -> None:
        self.source_db_name = source_db_name
        self.target_db_name = target_db_name
        self.source_table_name = source_table_name
        self.target_table_name = target_table_name
        self.schema = schema
        self.data = data
        
        if self.source_db_name:
            self.source_conn = sqlite3.connect(self.source_db_name)
            self.source_cursor = self.source_conn.cursor()

        if self.target_db_name:
            self.target_conn = sqlite3.connect(self.target_db_name)
            self.target_cursor = self.target_conn.cursor()
    

    def check_if_table_exists(self):
        table_exists = logger(f"{TableOperations.check_if_table_exists.__name__} method started, table to check: {self.table_name}, database:{self.db_name}", f"{TableOperations.check_if_table_exists.__name__}")
        table_exists.new_or_existing_run()
        query = f'''
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' 
                AND name="{self.target_table_name}"
                '''
        self.target_cursor.execute(query)
        result = self.target_cursor.fetchone()

        if result:
            table_exists = logger(f"Table {self.target_table_name} exists in {self.target_db_name}", f"{TableOperations.check_if_table_exists.__name__}")
            table_exists.new_or_existing_run()
            return True
        else:
            table_exists = logger(f"Table {self.target_table_name} does not exist in {self.target_db_name}", f"{TableOperations.check_if_table_exists.__name__}")
            table_exists.new_or_existing_run()
            return False
        

    def create_table(self):
        ct = logger(f"{TableOperations.create_table.__name__} method started, table to create: {self.table_name}, database:{self.db_name}", f"{TableOperations.create_table.__name__}")
        ct.new_or_existing_run()
        query = f'''
                CREATE TABLE IF NOT EXISTS {self.target_table_name} (
                {self.schema}
                )
                '''
        try:
            self.target_cursor.execute(query)
            self.target_conn.commit()
            ct2 = logger(f"{TableOperations.create_table.__name__} method finished, {self.target_table_name} is hereby created in database:{self.target_db_name}", f"{TableOperations.create_table.__name__}")
            ct2.new_or_existing_run()
        except Exception as e:
            ct2 = logger(f"{TableOperations.create_table.__name__} method finished, {self.target_table_name} has not been created in database:{self.target_db_name} due to exception: {e}", f"{TableOperations.create_table.__name__}")
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
        ct = logger(f"{TableOperations.drop_table.__name__} method started, table to drop: {self.target_table_name}, database:{self.target_db_name}", f"{TableOperations.drop_table.__name__}")
        ct.new_or_existing_run()
        query = f'''
                DROP TABLE {self.target_table_name}
                '''
        try:
            self.target_cursor.execute(query)
            self.target_conn.commit()
            ct = logger(f"Table {self.target_table_name} is hereby dropped, database:{self.target_db_name}", f"{TableOperations.drop_table.__name__}")
            ct.new_or_existing_run()
        except Exception as e:
            ct = logger(f"Table {self.target_table_name} has not been dropped due to exception: {e}, database:{self.target_db_name}", f"{TableOperations.drop_table.__name__}")
            ct.new_or_existing_run()


    def insert_df_into_table(self):
        ct = logger(f"{TableOperations.insert_df_into_table.__name__} method started, table to insert into: {self.table_name}, database:{self.db_name}", f"{TableOperations.insert_df_into_table.__name__}")
        ct.new_or_existing_run()

        try:
            self.data.to_sql(self.target_table_name, self.target_conn, if_exists='replace', index=False)
            ct2 = logger(f"Data was inserted into {self.target_table_name}, database:{self.target_db_name}", f"{TableOperations.insert_df_into_table.__name__}")
            ct2.new_or_existing_run()
            return True
        except Exception as e:
            ct2 = logger(f"Rows could not be inserted into {self.target_table_name} due to exception: {e}, database:{self.target_db_name}", f"{TableOperations.insert_df_into_table.__name__}")
            ct2.new_or_existing_run()
            return e


    def delete_all_from_table(self):
        ct = logger(f"{TableOperations.delete_all_from_table.__name__} method started, table to delete all from: {self.table_name}, database:{self.db_name}", f"{TableOperations.delete_all_from_table.__name__}")
        ct.new_or_existing_run()


        ct2 = logger(f"All rows have been deleted from {self.target_table_name}, database:{self.target_db_name}", f"{TableOperations.delete_all_from_table.__name__}")
        ct2.new_or_existing_run()

        ct2 = logger(f"Rows could not be deleted from {self.target_table_name} due to exception: {e}, database:{self.target_db_name}", f"{TableOperations.delete_all_from_table.__name__}")
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
                FROM {self.source_table_name}
                '''
        try:
            df = pd.read_sql_query(query, self.source_conn)
            ct2 = logger(f"{TableOperations.select_all_to_df.__name__} method finished, {self.source_table_name} is hereby created as a dataframe", f"{TableOperations.select_all_to_df.__name__}")
            ct2.new_or_existing_run()
            return df
        except Exception as e:
            ct2 = logger(f"{TableOperations.select_all_to_df.__name__} method finished, {self.source_table_name} has not been created as a dataframe due to exception: {e}", f"{TableOperations.select_all_to_df.__name__}")
            ct2.new_or_existing_run()


    def select_cols_to_df(self, layer):
        sctdf = logger(f"{TableOperations.select_cols_to_df.__name__} method started, table to select all from and insert into a pandas dataframe: {self.source_table_name}, database: {self.source_db_name}", f"{TableOperations.select_cols_to_df}")
        sctdf.new_or_existing_run()

        if layer == "bronze":
            cols = bronze_selected_columns.get(self.source_table_name)
        elif layer == "silver":
            cols = silver_selected_columns.get(self.source_table_name)
        
        coluns = ', '.join(cols)

        query = f'''
                SELECT {coluns}
                FROM {self.source_table_name}
                '''
        try:
            df = pd.read_sql_query(query, self.source_conn)
            ct2 = logger(f"{TableOperations.select_cols_to_df.__name__} method finished, {self.source_table_name} is hereby created as a dataframe", f"{TableOperations.select_cols_to_df.__name__}")
            ct2.new_or_existing_run()
            return df
        except Exception as e:
            ct2 = logger(f"{TableOperations.select_cols_to_df.__name__} method finished, {self.source_table_name} has not been created as a dataframe due to exception: {e}", f"{TableOperations.select_cols_to_df.__name__}")
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


