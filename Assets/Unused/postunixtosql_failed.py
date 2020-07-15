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

# Set POST URL
post_url = 'https://httpbin.org/post' # <-- Update with your POST URL

# Set Metric Names => Metrics you want to pull from request
metric_names = "Service_Level", "CurrNumberWaitingCalls", "Total_Calls_Answered", "Total_Abandoned"

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

# Time arrays to be used later
times = [fm, tm, fifm]
mytimes = ["five", "ten", "fifteen"]

#----------------------------------------------------------------------------------------------------------------------------------------
# Reset responses csvs
if op.isfile('responses.csv'):
    os.remove('responses.csv')
    print("Existing responses.csv, generating new file.")
    writeHeader = True

if op.isfile('five_response.csv'):
    os.remove('five_response.csv')
    print("Existing five_responses.csv, generating new file.")
    fiveHeader = True

if op.isfile('ten_response.csv'):
    os.remove('ten_response.csv')
    print("Existing ten_response.csv, generating new file.")
    tenHeader = True

if op.isfile('fifteen_response.csv'):
    os.remove('fifteen_response.csv')
    print("Existing fifteen_response.csv, generating new file.")
    fifteenHeader = True

#----------------------------------------------------------------------------------------------------------------------------------------
# Script to pull down latest 'API Objects' Table.  ****This section can be commented out if you're using the local objects.csv****

# """" <-- Remove pound signs above and below script if using local .csv
table = pd.read_csv(csv_url)
keep = ['Objects']
new_file = table[keep]
new_file.to_csv('objects.csv', sep=',', index=False)
# """"

#----------------------------------------------------------------------------------------------------------------------------------------
# Parse objects and appy each one to each POST Request
with open('objects.csv') as csvfile:
    reader = csv.reader(csvfile)
    next(csvfile)
    for x in csvfile:
        for y in times:
            for z in mytimes:
                myobject = x
                myobject.strip()
                myobject = myobject[:-1] # <-- Remove trailing spaces from my object.  This code can be removed if using local .csv.

                """
                # Append Unix times and current object to JSON body
                my_five_json = {"filters":{"objectname":[myobject]},"from":fm,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":[metric_names]}
                my_ten_json = {"filters":{"objectname":[myobject]},"from":tm,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":[metric_names]}
                my_fifteen_json = {"filters":{"objectname":[myobject]},"from":fifm,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":[metric_names]}

                #Make post request with JSON body
                five_request = requests.post(post_url, json=my_five_json)
                ten_request = requests.post(post_url, json=my_ten_json)
                fif_request = requests.post(post_url, json=my_fifteen_json)

                five_response = five_request.json()
                ten_response = ten_request.json()
                fif_response = fif_request.json()

                """

                #make POST request
                my_json = {"filters":{"objectname":[myobject]},"from":y,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":[metric_names]}
                my_request = requests.post(post_url, json=my_json)
                my_response = my_request.json()

                #----------------------------------------------------------------------------------------------------------------------------------------
                #Write back JSON to .csv

                #Set Dataframe as JSON response
                mydata = pd.read_json("C:\Projects\PayPal\Assets\sampledata\SampleResponse.json") # <-- This field should be x in production.
                df = pd.DataFrame(mydata)

                #drop Unit row from .csv
                df = df[:-1]
                
                #Set Index name
                df.index.name = 'Objects'
                df.index.values[0] = myobject
                
                #generate .csv names
                mytime = z
                csvname = mytime.strip() + "_response.csv"
                print(csvname)

                #Evaluate whether to write header
                
                if fiveHeader is True & mytime == "five":
                    df.to_csv(csvname, mode='w', header=True)
                    fiveHeader = False
                    print("Writing header row to", csvname)
                if tenHeader is True & mytime == "ten":
                    df.to_csv(csvname, mode='w', header=True)
                    tenHeader = False
                    print("Writing header row to", csvname)
                if fifteenHeader is True & mytime == "fifteen":
                    df.to_csv(csvname, mode='w', header=True)
                    fifteenHeader = False
                    print("Writing header row to", csvname)
                else:
                    df.to_csv(csvname, mode='a', header=False)
                    print("Appending ", myobject, "to ", csvname)
                
                
                #print("Status Codes:  ", "fiveminrequest:", five_request.status_code, "tenminrequest:", ten_request.status_code, "fifteenminrequest:", fif_request.status_code, "  -- For Object: ", myobject)
                #print(my_five_json)

        

#----------------------------------------------------------------------------------------------------------------------------------------
# Open SQL Connection, upload .csv to SQL.

"""
# ****IMPORTANT**** - YOU NEED TO UPDATE THE SERVER AND DATABASE FIELDS ↓ BELOW ↓
print("Connecting to SQL")
conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=<YOURSERVERNAME>;' # <-- Need to update Server name (probably something like <yourServerName\SQLExpress)
                        'Database=<YOURDATABASENAME>;' # <-- Need to update DB name
                        'Trusted_Connection=yes;')

print("Successfully Connected to SQL ")

# Update the File path below to read from your response.csv
read_responses = pd.read_csv(r'responses.csv') # <-- May need to update file path

cursor = conn.cursor()

print("Clear out existing sql table to make room for updated .csv.")
cursor.execute("DELETE FROM <YOURDBNAME>") # <--Update with your DB name

print("Updating SQL table with responses.csv")
for index, row in read_responses.iterrows():
    print(row)
    # Make sure you update your DB name below ↓
    cursor.execute("INSERT INTO <YOURDBNAME>([Objects],[Service_Level],[CurrNumberWaitingCalls],[Total_Calls_Answered],[Total_Abandoned]) values(?,?,?,?,?)", row['Objects'], row['Service_Level'], row['CurrNumberWaitingCalls'], row['Total_Calls_Answered'], row['Total_Abandoned'])

conn.commit()
cursor.close()
conn.close()
"""


#----------------------------------------------------------------------------------------------------------------------------------------
