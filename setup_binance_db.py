import requests
import pandas as pd 
import datetime 
import urllib.parse
import hashlib
import hmac
import time
import sqlite3
import os
from update_crypto_db import product_list, base_currency
from not_secrets import binanca_key,binanca_secret


dir= "E:\Crypto_DB\Binance_DB\Binance_US"
if not os.path.exists(dir):
    os.makedirs(dir)



api_url = "https://api.binance.us"
api_key=binanca_key
secret_key=binanca_secret
uri_path = "/sapi/v1/system/status"
data = {
   "timestamp": int(round(time.time() * 1000)),
}

# get binanceus signature
def get_binanceus_signature(data, secret):
   postdata = urllib.parse.urlencode(data)
   message = postdata.encode()
   byte_key = bytes(secret, 'UTF-8')
   mac = hmac.new(byte_key, message, hashlib.sha256).hexdigest()
   return mac

# Attaches auth headers and returns results of a POST request
def binanceus_request():
    uri_path
    data
    api_key
    api_sec= secret_key
    headers = {}
    headers['X-MBX-APIKEY'] = api_key
    signature = get_binanceus_signature(data, api_sec)
    params={
        **data,
        "signature": signature,
        }
    req = requests.get((api_url + uri_path), params=params, headers=headers)
    
    return req.text

def get_binace_pairs():
    binance_official_symbol_list=[]
    binance_pairs= []

    resp = requests.get('https://api.binance.us/api/v3/exchangeInfo')
    resp= resp.json()
    for key, value in resp.items():
        if key == "symbols":
            df=pd.json_normalize(data=resp["symbols"])
            for x in df.symbol: 
                binance_official_symbol_list.append(x)
                
    print(binance_official_symbol_list)
    for x in product_list: 
        for y in base_currency:
            symbol= str(x+y)
            if symbol in binance_official_symbol_list: 
                binance_pairs.append(symbol)
            else: 
                print(str(symbol)+" Is not in the Binance.US Tradeable List")
    return binance_pairs

def get_binace_last_date(table_name):

    db_file= os.path.join(dir,"binance_us.db")
    conn= sqlite3.connect(db_file)
    c= conn.cursor()

    # Check if table exists
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    table_names = c.fetchall()
    if table_name not in [table[0] for table in table_names]:
        return None

    # Query last date in table
    query = 'SELECT MAX(date) FROM {}'.format(table_name)
    c.execute(query)
    last_date = c.fetchone()[0]

    # Close connection to database
    c.close()
    conn.close()

    last_date= pd.to_datetime(last_date)

    #get the current time in utc
    current_time= datetime.datetime.utcnow()

    #create the invterval so it can tell if it has an 5 hour time to get
    interval = datetime.timedelta(hours=5)

# Check if the last date is at least 6 hours ago
    if current_time - last_date >= interval:
        return last_date
    else:
        return "Not yet"

def run_binance_DB():
    ohlc_url= "https://api.binance.us/api/v3/aggTrades"
    if binanceus_request()== '{"status":0}':
        time.sleep(5)
        limit = 1000
        binance_interval= "1h"
        symbols= get_binace_pairs()
        time.sleep(5) 
        db_file= os.path.join(dir,"binance_us.db")
        conn= sqlite3.connect(db_file)
        
        for pair in symbols:
             
            df_list = []
            table_name= pair

            if get_binace_last_date(table_name) == None: 
                start_date = "2019-06-03"
                end_date = "2022-05-03"

                # Convert UTC time to Unix time (seconds since Jan 1, 1970)
                date_range = pd.date_range(start_date, end_date,freq="MS")
                # now go through each date
                for date in date_range:
                    print(date)
                    print(str(pair)+ " "+ str(date)) 
                    first_date= round((time.mktime(date.timetuple())))
                        #api limits 1000 candles /request fyi 
                    second_time= date+pd.Timedelta("1000 minute")
                    second_date= round((time.mktime(second_time.timetuple())))
                    first_date= first_date *1000
                    second_date= second_date *1000
                    

                    response= requests.get("https://api.binance.us/api/v3/klines?symbol="+str(table_name)+"&interval="+str(binance_interval)+"&startTime="+str(first_date))

                    
                    time.sleep(1.5)
                    if str(response) == "<Response [200]>":
                        first_df= pd.DataFrame(response.json())
                        #print(first_df)
                        df_list.append(first_df)
                    else: 
                        print(response)
                        continue

            elif get_binace_last_date(table_name) == "Not yet":
                print("It has been less than 5 hours since this symbol was created: " + str(pair))
                continue
            else: 

                start_date = get_binace_last_date(table_name)
                start_date = pd.to_datetime(start_date)
                end_date = datetime.datetime.utcnow()

                # Subtract one day from UTC time
                end_date -= datetime.timedelta(hours=5)
                # Set time to the last minute of the previous day
                end_date = end_date.replace(hour=23, minute=59, second=0, microsecond=0)
                # Convert UTC time to Unix time (seconds since Jan 1, 1970)
                date_range = pd.date_range(start_date, end_date,freq="1000min")
                for date in date_range:
                    print(str(pair)+ " "+ str(date))  
                    first_date= round((time.mktime(date.timetuple())))
                        #api limits 300 candles /request fyi 
                    second_time= date+pd.Timedelta("1000 minute")
                    second_date= round((time.mktime(second_time.timetuple())))
                    
                    #turn times into milliseconds 
                    first_date= first_date *1000
                    second_date= second_date *1000

                    response= requests.get("https://api.binance.us/api/v3/klines?symbol="+str(table_name)+"&interval="+str(binance_interval)+"&startTime="+str(first_date)+"&endTime="+str(second_date)+"&limit="+str(limit))

                    
                    time.sleep(1.5)
                    if str(response) == "<Response [200]>":
                        first_df= pd.DataFrame(response.json())
                        
                        df_list.append(first_df)
                    else: 
                        print(response)
                        continue
            print(table_name)
            second_df= pd.concat(df_list)

            #create column names
            second_df.columns=['date',"open","high","low","close","volume","Closetime","Quoteassetvolume","Numberoftrades","Takerbuybaseassetvolume","Takerbuyquoteassetvolume","Ignore"]
            
            #change format from unix to utc 
            second_df['date']= pd.to_datetime(second_df['date'], unit='ms',origin= 'unix')
            second_df['Closetime']= pd.to_datetime(second_df['Closetime'], unit='ms',origin= 'unix')
            #print(second_df)


            #to sql 
            second_df.to_sql(str(table_name),
                                conn, 
                                if_exists="append",
                                index=False)

    else: 
        x = "The server is offline"
        return x
print(run_binance_DB())
