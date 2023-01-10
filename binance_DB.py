import json 
import requests
import pandas as pd 
import datetime 
import urllib.parse
import hashlib
import hmac
import base64
import time
import sqlite3
import os
from not_secrets import binanca_key,binanca_secret

dir= "E:\Crypto_DB\Binance_DB\Binance_US"
api_url = "https://api.binance.us"
api_key=binanca_key
secret_key=binanca_secret
uri_path = "/sapi/v1/system/status"
data = {"timestamp": int(round(time.time() * 1000)),}

def get_binanceus_signature(data, secret):
   postdata = urllib.parse.urlencode(data)
   message = postdata.encode()
   byte_key = bytes(secret, 'UTF-8')
   mac = hmac.new(byte_key, message, hashlib.sha256).hexdigest()
   return mac

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

def get_binance_pairs():
    # Connect to the database
    db_file= os.path.join(dir,"binance_us.db")
    conn= sqlite3.connect(db_file)
    c = conn.cursor()

    # Get the list of table names
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    official_binance_paris = [table[0] for table in c.fetchall()]

    # Close the connection
    conn.close()

    return official_binance_paris


def get_binance_last_date(table_name):

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

    #create the invterval so it can tell if it has an 18 hour time to get
    interval = datetime.timedelta(hours=18)

    # Check if the last date is at least 18 hours ago
    if current_time - last_date >= interval:
        return last_date
    else:
        return "Not yet"

def run_binance_DB():
    
    if binanceus_request()== '{"status":0}':
        time.sleep(5)
        limit = 1000
        binance_interval= "1m"
        symbol_list= get_binance_pairs()
        db_file= os.path.join(dir,"binance_us.db")
        conn= sqlite3.connect(db_file)
        

        # Subtract 17 hours from UTC time
        current_time = datetime.datetime.utcnow()
        interval = datetime.timedelta(hours=17)

        # Subtract the interval from the current time to get 5 hours behind
        time_minus_5_hours = current_time - interval
        time_string = time_minus_5_hours.strftime("%Y-%m-%d %H:%M:%S")
        end_date= datetime.datetime.strptime(time_string,"%Y-%m-%d %H:%M:%S")
        end_date= end_date.replace(second=0)
        for table_name in symbol_list:

            if str(get_binance_last_date(table_name)) == "Not yet":
                print("It has not been more than 18 hours for Binance Price to be updated for pair: "+ str(table_name))
                continue
            else: 
                first_date= get_binance_last_date(table_name)

                date_index= pd.date_range(first_date,end_date,freq="1000min")
                for date in date_index:
                    print(str(date)+ " "+ str(table_name))


                    first_date= round((time.mktime(date.timetuple())))
                        #api limits 1000 candles /request fyi 
                    second_time= date+pd.Timedelta("1000 minute")
                    second_date= round((time.mktime(second_time.timetuple())))
                    #turn times into milliseconds 
                    first_date= first_date *1000
                    second_date= second_date *1000

                    response= requests.get("https://api.binance.us/api/v3/klines?symbol="+str(table_name)+"&interval="+str(binance_interval)+"&startTime="+str(first_date)+"&endTime="+str(second_date)+"&limit="+str(limit))
                    time.sleep(2)
                    
                    if str(response) != "<Response [200]>":
                        print(response)
                        continue
                    else: 
                        first_df= pd.DataFrame(response.json())
                        first_df.columns=['date',"open","high","low","close","volume","Closetime","Quoteassetvolume","Numberoftrades","Takerbuybaseassetvolume","Takerbuyquoteassetvolume","Ignore"]
            
                        #change format from unix to utc 
                        first_df['date']= pd.to_datetime(first_df['date'], unit='ms',origin= 'unix')
                        first_df['Closetime']= pd.to_datetime(first_df['Closetime'], unit='ms',origin= 'unix')
                        first_df.to_sql(str(table_name),
                                conn, 
                                if_exists="append",
                                index=False)
    else: 
        print("The server status is offline or check clock synchronization")

print(run_binance_DB())
