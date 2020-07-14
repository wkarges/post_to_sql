import time
import csv
import sqlalchemy
import json
import requests
import pandas as pd
import os.path as op
import sqlite3 as sq
import os
import pyodbc
import urllib.request as urlr

# Make Time Calculations
now = int(time.time())

def fivemins():
    return int(time.time()-(5 * 60))

def tenmins():
    return int(time.time()-(10 * 60))

def fifteenmins():
    return int(time.time()-(15 * 60))

fm = fivemins()
tm = tenmins()
fifm = fifteenmins()

#--------------------------------------------------------------------------------------------------------------------

# Reset .csv
if op.isfile('responses.csv'):
    os.remove('responses.csv')
    print("Existing responses.csv, generating new file.")

#Set POST URL
url = 'https://httpbin.org/post'

# Parse objects and appy each one to each POST Request
with open('objects.csv') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)
    for x in reader:
        myobject = x

        if op.isfile('responses.csv'):
            writeHeader = False
        else:
            writeHeader = True

        # Append Unix times and current object to JSON body
        # ***Note to self*** may need to imput concatinate function to remove the brackets from myobject
        my_five_json = {"filters":{"objectname":[myobject]},"from":fm,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":["Service_Level", "CurrNumberWaitingCalls", "Total_Calls_Answered", "Total_Abandoned"]}
        my_ten_json = {"filters":{"objectname":[myobject]},"from":tm,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":["Service_Level", "CurrNumberWaitingCalls", "Total_Calls_Answered", "Total_Abandoned"]}
        my_fifteen_json = {"filters":{"objectname":[myobject]},"from":fifm,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":["Service_Level", "CurrNumberWaitingCalls", "Total_Calls_Answered", "Total_Abandoned"]}

        #Make post request with JSON body
        post_request = requests.post(url, json=my_five_json)
        my_response = post_request.json()

        print("Status Code:  ", post_request.status_code, myobject)
        
        #----------------------------------------------------------------------
        #Write back JSON to .csv

        #Set Dataframe as JSON response
        data = pd.read_json("C:\Projects\PayPal\SampleResponse.json") # <-- This field should be my_response in production.
        df = pd.DataFrame(data)

        #drop Unit row from .csv
        df = df[:-1]
        
        #Set Index name
        df.index.name = 'Objects'
        df.index.values[0] = myobject
        
        #Evaluate whether to write header
        if writeHeader is True:
            df.to_csv('responses.csv', mode='w', header=True)
            writeHeader = False
            print("Writing header row to response.csv")
        else:
            df.to_csv('responses.csv', mode='a', header=False)
            print("Appending ", myobject, "to responses.csv")

#--------------------------------------------------------------------------------------------------------------------
# Open SQL Connection, upload .csv to SQL.

# IMPORTANT - YOU NEED TO UPDATE THE SERVER AND DATABASE FIELDS BELOW ↓
print("Connecting to SQL")
conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=DESKTOP-V8I0892\SQLEXPRESS;' # <-- Need to update Server name
                        'Database=PayPalTest;' # <-- Need to update DB name
                        'Trusted_Connection=yes;')

print("Successfully Connected to SQL ")

# Update the File path below to read from your response.csv
read_responses = pd.read_csv(r'C:\Projects\PayPal\responses.csv') # <-- May need to update file path

cursor = conn.cursor()

print("Clear out existing sql table to make room for updated .csv.")
cursor.execute("DELETE FROM PayPalTest2")

print("Updating SQL table with responses.csv")
for index, row in read_responses.iterrows():
    print(row)

    cursor.execute("INSERT INTO PayPalTest2([Objects],[Service_Level],[CurrNumberWaitingCalls],[Total_Calls_Answered],[Total_Abandoned]) values(?,?,?,?,?)", row['Objects'], row['Service_Level'], row['CurrNumberWaitingCalls'], row['Total_Calls_Answered'], row['Total_Abandoned'])

conn.commit()
cursor.close()
conn.close()



#--------------------------------------------------------------------------------------------------------------------
