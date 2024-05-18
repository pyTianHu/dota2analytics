import sys
sys.path.append('../dota2')

import pandas as pd
from utils.utils import logger
from classes.tableoperations import TableOperations
from utils.silver_utils import rows_isin
from utils.gold_utils import column_rename, selected_columns as gold_selected_columns, table_constraints as gold_table_constraints, table_rename as gold_table_rename


class DataFrameOperations():
    # get data from sqlite3 via tableoperations df from table
    # do something with it according to silver_utils and gold_utils
    # insert into table via tableoperations

    def __init__(self, source_db_name, table_name, target_db_name = None) -> None:
        self.source_db_name = source_db_name
        self.table_name = table_name

        self.table_object = TableOperations(self.source_db_name, self.table_name)
        self.df = self.table_object.select_all_to_df()

        
    def return_df(self):
        return self.df
    
    def filter_rows(self):
        if self.table_name not in rows_isin:
            pass
        else:
            filter_criteria = rows_isin[self.table_name]
            for field_name, criteria in filter_criteria.items():
                for action, values in criteria.items():
                    if action == "drop":
                        self.df = self.df[~self.df[field_name].isin(values)]
                    elif action == "select":
                        self.df = self.df[self.df[field_name].isin(values)]
                    else:
                        return(f"This action is not known {action}")
        
        return self.df
    
    def silver_to_gold_df(self):
        df = self.df
        table_name = self.table_name
        source_db_name = self.source_db_name
        table_rename = gold_table_rename.get(table_name)

        # select columns
        table_object = TableOperations(source_db_name,table_name)
        df = table_object.select_cols_to_df('gold')

        # rename columns
        for column in column_rename[table_rename].items():
            df.rename(columns = {column[0]: column[1]}, inplace=True)


        for column,constraints in gold_table_constraints[table_rename].items():
            constraint = constraints[0]
            if constraint == "TIMESTAMP" or constraint == "DATETIME":
                try:
                    df[column] = pd.to_datetime(df[column], unit = 's')
                except:
                    df[column] = pd.to_datetime(df[column])
            #if constraint == "STRING":
            #    df[column] = df[column].astype(str)
            if constraint == "INTEGER":
                df[column] = df[column].astype("Int64")

        return df, table_rename


