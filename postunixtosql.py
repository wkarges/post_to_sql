########################################################################################################################################

# Written by William Karges - July 2020
# The purpose of this script is to consolidate all of the client's desired data into a single SQL database from numerous POST requests.
# Go to https://github.com/wkarges/post_to_sql for full documenation.

########################################################################################################################################
# SECTION 1. Import the necessary python libraries.

import time
import csv
import json
import requests
import pandas as pd
import os.path as op 
import os
import pyodbc

#----------------------------------------------------------------------------------------------------------------------------------------
# SECTION 2. Global Variables

# 'API Objects' Cloud Table URL -> Specifically your desired objects to be aggregated.  Not needed if you're using a local objects.csv
csv_url = "<YOURCLOUDTABLE>" # <-- Update with your FWI Cloud Table .csv

# Set POST URL
post_url = '<YOURPOSTURL>' # <-- Update with your POST URL

# Open SQL connection, need to update Server and DB fields ↓ BELOW ↓
conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=<YOURSQL>\SQLEXPRESS;' # <-- Need to update Server name (probably something like <yourServerName\SQLExpress)
                        'Database=<YOURDB>;' # <-- Need to update DB name
                        'Trusted_Connection=yes;')

# If username/pass and/or remote connection is required for SQL, comment out the code above and use the script ↓ BELOW ↓
# conn = pyodbc.connect('DRIVER={FreeTDS};SERVER=<yourserver>;PORT=1433;DATABASE=your_db;UID=your_username;PWD=your_password;TDS_Version=7.2;')

# Set Metric Names => Metrics you want to pull from request 
# **Warning** If you modify these fields, you will also need to update the `myscript` SQL query in the last code section.
metric_names = "Service_Level", "CurrNumberWaitingCalls", "Total_Calls_Answered", "Total_Abandoned"

# Set File Headers **Important** Please carefully review the README before adjusting.
myfileheaders = ["a", "b", "c"]

# Set Asset filepath off current working directory
mypath = os.getcwd() + "\\Assets\\"

#----------------------------------------------------------------------------------------------------------------------------------------
# SECTION 3. Make Time Calculations. These may need to be adjusted per client request.

def fivemins():
    return int(time.time()-(5 * 60))

def tenmins():
    return int(time.time()-(10 * 60))

def fifteenmins():
    return int(time.time()-(15 * 60))

now = int(time.time())
atime = fivemins()
btime = tenmins()
ctime = fifteenmins()

#----------------------------------------------------------------------------------------------------------------------------------------
# SECTION 4. Reset responses csvs

for x in myfileheaders:
    thetime = x
    setpath = mypath + thetime.strip() + "_responses.csv"
    setcsv = thetime.strip() + "_responses.csv"
    print(setpath)

    if op.isfile(setpath):
        os.remove(setpath)
        print("Existing ", setcsv, " found, generating new file.")

#----------------------------------------------------------------------------------------------------------------------------------------
# SECTION 5. Script to pull down latest 'API Objects' Table.  ****This section can be commented out if you're using the local objects.csv****

# """" <-- Remove pound signs above and below script if using local .csv
table = pd.read_csv(csv_url)
keep = ['Objects']
new_file = table[keep]
new_file.to_csv(mypath+'objects.csv', sep=',', index=False)
# """"

#----------------------------------------------------------------------------------------------------------------------------------------
# SECTION 6. Parse objects and appy each one to each POST Request


with open(mypath+'objects.csv') as csvfile:
    reader = csv.reader(csvfile)
    next(csvfile)
    for x in csvfile:
        myobject = x
        myobject.strip()
        myobject = myobject[:-1] # <-- Remove trailing spaces from my object.  This code can be removed if using local .csv.               

        # Append Unix times and current object to JSON body
        my_five_json = {"filters":{"objectname":[myobject]},"from":atime,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":[metric_names]}
        my_ten_json = {"filters":{"objectname":[myobject]},"from":btime,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":[metric_names]}
        my_fifteen_json = {"filters":{"objectname":[myobject]},"from":ctime,"to":now,"channels":["voice"],"timeInterval":"all","metricNames":[metric_names]}

        #----------------------------------------------------------------------------------------------------------------------------------------
        # SECTION 6.1 Write back JSON to .csv

        for x in myfileheaders:
            curr_time = x
            my_json = "my_" + curr_time.strip() + "_json"
            my_request = requests.post(post_url, json=my_json)
            my_response = my_request.json()

            print(myobject, curr_time, "request: ", "Status Code ", my_request.status_code)
            
            #Set Dataframe as JSON response
            mydata = pd.read_json(my_response) # <-- This field should be my_response in production.

            df = pd.DataFrame(mydata)

            #drop Unit row from .csv
            df = df[:-1]
            
            # Set Index name
            df.index.name = 'Objects'
            df.index.values[0] = myobject

            # Set Dataframes
            if x == "a":
                adf = df
            if x == "b":
                bdf = df
            if x == "c":
                cdf = df
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # SECTION 6.2 Evaluate whether to write header

        a_res = 'a_responses.csv'
        b_res = 'b_responses.csv'
        c_res = 'c_responses.csv'

        print("Compiling .csv for", myobject)
        if op.isfile(mypath+a_res):
            aHeader = False
        else:
            aHeader = True
        if op.isfile(mypath+b_res):
            bHeader = False
        else:
            bHeader = True
        if op.isfile(mypath+c_res):
            cHeader = False
        else:
            cHeader = True

        if aHeader is True:
            adf.to_csv(mypath+a_res, mode='w', header=True)
            aHeader = False
        else:
            adf.to_csv(mypath+a_res, mode='a', header=False)
        if bHeader is True:
            bdf.to_csv(mypath+b_res, mode='w', header=True)
            bHeader = False
        else:
            bdf.to_csv(mypath+b_res, mode='a', header=False)
        if cHeader is True:
            cdf.to_csv(mypath+c_res, mode='w', header=True)
            cHeader = False
        else:
            cdf.to_csv(mypath+c_res, mode='a', header=False)

#----------------------------------------------------------------------------------------------------------------------------------------
# SECTION 7. Upload .csv to SQL.

cursor = conn.cursor()

for x in myfileheaders:
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
