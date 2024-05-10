import sys
sys.path.append('../dota2')

import pandas as pd
from utils.utils import logger
from classes.tableoperations import TableOperations
from utils.silver_utils import rows_isin


class DataFrameOperations():
    # get data from sqlite3 via tableoperations df from table
    # do something with it according to silver_utils and gold_utils
    # insert into table via tableoperations

    def __init__(self, db_name, table_name) -> None:
        self.db_name = db_name
        self.table_name = table_name

        self.table_object = TableOperations(self.db_name, self.table_name)
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


