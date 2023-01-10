import pandas as pd 
import os 
import sqlite3

dir= "E:\Crypto_DB\Binance_DB\Binance_US"
db_file= os.path.join(dir,"binance_us.db")
conn= sqlite3.connect(db_file)
cursor = conn.cursor()
tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()

# Iterate through the tables
for table in tables:
    # Get the table name
    table_name = table[0]
    print(table_name)
    query = f'SELECT * FROM {table_name} LIMIT 1'
    df = pd.read_sql(query, conn)
    print(df)

    # Write the DataFrame to a CSV file with the table name as the file name
    df.to_csv(f'Binance_ohlc/{table_name}.csv', index=False)
conn.close()
