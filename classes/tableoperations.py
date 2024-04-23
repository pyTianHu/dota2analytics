import sqlite3

class TableOperations:
    def __init__(self, db_name, table_name, schema = None, data = None) -> None:
        self.db_name = db_name
        self.table_name = table_name
        self.schema = schema
        self.data = data
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
    

    def create_table(self):
        query = f''' 
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                {self.schema}
                )
                '''
        try:
            self.cursor.execute(query)
            self.conn.commit()
            return "Table is hereby created"
        except Exception as e:
            return f"Table has not been created - {e}"
    

    def add_new_column(self):
        pass


    def remove_existing_column(self):
        pass


    def drop_table(self):
        pass


    def insert_df_into_table(self):
        pass


    def delete_all_from_table(self):
        pass


    def delete_from_table_where(self):
        pass


    def select_sample_from_table(self):
        pass


    def print_schema(self):
        pass


