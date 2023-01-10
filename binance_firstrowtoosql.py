import pandas as pd 
import os 
import sqlite3

dir= "E:\Crypto_DB\Binance_DB\Binance_US"
db_file= os.path.join(dir,"binance_us.db")
for root, dirs, files in os.walk("Binance_ohlc"):
    for file in files:
        symbol= file.replace(".csv","") 
        print(symbol)
        df= pd.read_csv(os.path.join(root,file),header=0)
        dir= "E:\Crypto_DB\Binance_DB\Binance_US"
        db_file= os.path.join(dir,"binance_us.db")
        conn= sqlite3.connect(db_file)
        df.to_sql(str(symbol),
                                conn, 
                                if_exists="append",
                                index=False)
