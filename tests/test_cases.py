import sys
sys.path.append('../dota2')

from classes.tableoperations import TableOperations
#from data_ingestion import heroes_ingestion
import sqlite3
from utils.utils import open_schemas


#test case for checking if heroes_raw_test is created successfully on dot_dev.db
query = "select * from heroes_raw_test"
conn = sqlite3.connect('dot_dev.db')
cursor = conn.cursor()
cursor.execute(query)
data = cursor.fetchall()
print(data)