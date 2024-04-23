import sqlite3
import pandas as pd

def test_case_d10(db_name, table_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    query = f"SELECT * FROM {table_name}"

    df = pd.read_sql_query(query, conn)

    print(df)

test_case_d10('dot_raw_prod.db', 'heroes')