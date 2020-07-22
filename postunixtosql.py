########################################################################################################################################
# Written by William Karges - July 2020

########################################################################################################################################
"""
The purpose of this script is to consolidate all of the client's desired data into a single SQL database from numerous POST requests.
"""

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
import pathlib

#----------------------------------------------------------------------------------------------------------------------------------------
# Global Variables

# 'API Objects' Cloud Table URL -> Specifically your desired objects to be aggregated.  Not needed if you're using a local objects.csv
csv_url = "https://api.fwicloud.com/library/v1/company/c12c4596-b635-42d9-9392-2da67b69a25c/tables/39c648cd-e774-446b-82f8-d6dad449529e/csv" # <-- Update with your FWI Cloud Table .csv

# Set POST URL
post_url = 'https://httpbin.org/post' # <-- Update with your POST URL

# Set Metric Names => Metrics you want to pull from request
metric_names = "Service_Level", "CurrNumberWaitingCalls", "Total_Calls_Answered", "Total_Abandoned"

# Open SQL connection, need to update Server and DB fields ↓ BELOW ↓
conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=DESKTOP-V8I0892\SQLEXPRESS;' # <-- Need to update Server name (probably something like <yourServerName\SQLExpress)
                        'Database=post_test;' # <-- Need to update DB name
                        'Trusted_Connection=yes;')

# Set different times **DO NOT MODIFY**
mytimes = ["five", "ten", "fifteen"]

# Set Asset filepath
mypath = os.getcwd() + "\\Assets\\"

#----------------------------------------------------------------------------------------------------------------------------------------
# Make Time Calculations, These may need to be adjusted per client request

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

# Time arrays currently unused
times = [fm, tm, fifm]

#----------------------------------------------------------------------------------------------------------------------------------------
# Reset responses csvs
for x in mytimes:
    thetime = x
    setpath = mypath + thetime.strip() + "_responses.csv"
    setcsv = thetime.strip() + "_responses.csv"
    print(setpath)

    if op.isfile(setpath):
        os.remove(setpath)
        print("Existing ", setcsv, " found, generating new file.")

#----------------------------------------------------------------------------------------------------------------------------------------
# Script to pull down latest 'API Objects' Table.  ****This section can be commented out if you're using the local objects.csv****

# """" <-- Remove pound signs above and below script if using local .csv
table = pd.read_csv(csv_url)
keep = ['Objects']
new_file = table[keep]
new_file.to_csv(mypath+'objects.csv', sep=',', index=False)
# """"

#----------------------------------------------------------------------------------------------------------------------------------------
# Parse objects and appy each one to each POST Request


with open(mypath+'objects.csv') as csvfile:
    reader = csv.reader(csvfile)
    next(csvfile)
    for x in csvfile:
        myobject = x
        myobject.strip()
        myobject = myobject[:-1] # <-- Remove trailing spaces from my object.  This code can be removed if using local .csv.               

        # Append Unix times and current object to JSON body
        my_five_json = {"filters":{"objectname":[myobject]},"from":fm,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":[metric_names]}
        my_ten_json = {"filters":{"objectname":[myobject]},"from":tm,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":[metric_names]}
        my_fifteen_json = {"filters":{"objectname":[myobject]},"from":fifm,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":[metric_names]}

        #----------------------------------------------------------------------------------------------------------------------------------------
        #Write back JSON to .csv

        for x in mytimes:
            curr_time = x
            my_json = "my_" + curr_time.strip() + "_json"
            my_request = requests.post(post_url, json=my_json)
            my_response = my_request.json()

            print(myobject, curr_time, " -- ", "request: ", "Status Code ", my_request.status_code)
            
            #Set Dataframe as JSON response
            mydata = pd.read_json("C:\Projects\PayPal\Assets\sampledata\SampleResponse.json") # <-- This field should be my_response in production.

            df = pd.DataFrame(mydata)

            #drop Unit row from .csv
            df = df[:-1]
            
            # Set Index name
            df.index.name = 'Objects'
            df.index.values[0] = myobject

            # Set Dataframes
            if x == "five":
                fivedf = df
            if x == "ten":
                tendf = df
            if x == "fifteen":
                fifdf = df
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        #Evaluate whether to write header

        print("Compiling .csv for", myobject)
        if op.isfile(mypath+'five_responses.csv'):
            fiveHeader = False
        else:
            fiveHeader = True
        if op.isfile(mypath+'ten_responses.csv'):
            tenHeader = False
        else:
            tenHeader = True
        if op.isfile(mypath+'fifteen_responses.csv'):
            fifteenHeader = False
        else:
            fifteenHeader = True

        if fiveHeader is True:
            fivedf.to_csv(mypath+'five_responses.csv', mode='w', header=True)
            fiveHeader = False
        else:
            fivedf.to_csv(mypath+'five_responses.csv', mode='a', header=False)
        if tenHeader is True:
            tendf.to_csv(mypath+'ten_responses.csv', mode='w', header=True)
            tenHeader = False
        else:
            tendf.to_csv(mypath+'ten_responses.csv', mode='a', header=False)
        if fifteenHeader is True:
            fifdf.to_csv(mypath+'fifteen_responses.csv', mode='w', header=True)
            fifteenHeader = False
        else:
            fifdf.to_csv(mypath+'fifteen_responses.csv', mode='a', header=False)

#----------------------------------------------------------------------------------------------------------------------------------------
# Upload .csv to SQL.

cursor = conn.cursor()

for x in mytimes:
    currdb_time = x
    mycsv = currdb_time.strip() + "_responses.csv"
    mytable = currdb_time.strip() + "_table"
    thepath = mypath + mycsv
    read_responses = pd.read_csv(thepath)
    
    print("Clear out existing ", mytable, "to make room for updated csv.")
    cursor.execute("DELETE FROM " + mytable.strip())

    print("Updating ", mytable, "with", mycsv)
    for index, row in read_responses.iterrows():
        print(row)
        myscript = "INSERT INTO " + mytable.strip() + "([Objects],[Service_Level],[CurrNumberWaitingCalls],[Total_Calls_Answered],[Total_Abandoned]) values(?,?,?,?,?)"
        cursor.execute(myscript, row['Objects'], row['Service_Level'], row['CurrNumberWaitingCalls'], row['Total_Calls_Answered'], row['Total_Abandoned'])

conn.commit()
cursor.close()
conn.close()

#----------------------------------------------------------------------------------------------------------------------------------------
