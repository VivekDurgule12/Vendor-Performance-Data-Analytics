import sqlite3
import pandas as pd

# connect to db
conn = sqlite3.connect("inventory.db")

# get all table names
tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)

for table in tables['name']:
    df = pd.read_sql_query(f"SELECT * FROM {table};", conn)
    df.to_csv(f"{table}.csv", index=False)

conn.close()
