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
import io

#----------------------------------------------------------------------------------------------------------------------------------------
# Global Variables

# 'API Objects' Cloud Table URL -> Specifically your desired objects to be aggregated.  Not needed if you're using a local objects.csv
csv_url = "https://api.fwicloud.com/library/v1/company/c12c4596-b635-42d9-9392-2da67b69a25c/tables/39c648cd-e774-446b-82f8-d6dad449529e/csv" # <-- Update with your FWI Cloud Table .csv

#Set POST URL
post_url = 'https://httpbin.org/post'

#----------------------------------------------------------------------------------------------------------------------------------------
# Make Time Calculations

def fivemins():
    return int(time.time()-(5 * 60))

def tenmins():
    return int(time.time()-(10 * 60))

def fifteenmins():
    return int(time.time()-(15 * 60))

now = int(time.time())
fm = fivemins()
tm = tenmins()
fifm = fifteenmins()

#----------------------------------------------------------------------------------------------------------------------------------------
# Script to pull down latest 'API Objects' Table.  ****This section can be commented out if you're using the local objects.csv****

table = pd.read_csv(csv_url)
keep = ['Objects']
new_file = table[keep]
new_file.to_csv('objects.csv', sep=',', index=False)

#----------------------------------------------------------------------------------------------------------------------------------------
# Reset responses.csv
if op.isfile('responses.csv'):
    os.remove('responses.csv')
    print("Existing responses.csv, generating new file.")


# Parse objects and appy each one to each POST Request
with open('objects.csv') as csvfile:
    reader = csv.reader(csvfile)
    next(csvfile)
    for x in csvfile:
        myobject = x
        myobject.strip()
        myobject = myobject[:-1] # <-- Remove trailing spaces from my object.  This code can be removed if using local .csv.

        if op.isfile('responses.csv'):
            writeHeader = False
        else:
            writeHeader = True

        # Append Unix times and current object to JSON body
        my_five_json = {"filters":{"objectname":[myobject]},"from":fm,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":["Service_Level", "CurrNumberWaitingCalls", "Total_Calls_Answered", "Total_Abandoned"]}
        my_ten_json = {"filters":{"objectname":[myobject]},"from":tm,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":["Service_Level", "CurrNumberWaitingCalls", "Total_Calls_Answered", "Total_Abandoned"]}
        my_fifteen_json = {"filters":{"objectname":[myobject]},"from":fifm,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":["Service_Level", "CurrNumberWaitingCalls", "Total_Calls_Answered", "Total_Abandoned"]}

        #Make post request with JSON body
        post_request = requests.post(post_url, json=my_five_json)
        my_response = post_request.json()

        # print("Status Code:  ", post_request.status_code, myobject)
        print(my_five_json)
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        #Write back JSON to .csv

        #Set Dataframe as JSON response
        mydata = pd.read_json("C:\Projects\PayPal\Assets\sampledata\SampleResponse.json") # <-- This field should be my_response in production.
        df = pd.DataFrame(mydata)

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

#----------------------------------------------------------------------------------------------------------------------------------------
# Open SQL Connection, upload .csv to SQL.

# ****IMPORTANT**** - YOU NEED TO UPDATE THE SERVER AND DATABASE FIELDS ↓ BELOW ↓
print("Connecting to SQL")
conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=DESKTOP-V8I0892\SQLEXPRESS;' # <-- Need to update Server name
                        'Database=PayPalTest;' # <-- Need to update DB name
                        'Trusted_Connection=yes;')

print("Successfully Connected to SQL ")

# Update the File path below to read from your response.csv
read_responses = pd.read_csv(r'responses.csv') # <-- May need to update file path

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



#----------------------------------------------------------------------------------------------------------------------------------------
