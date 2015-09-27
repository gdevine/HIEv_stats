'''
Python script to return a series of stats from the HIEv database

Author: Gerard Devine
Date: September 2015


- Note: A valid HIEv API key is required  

'''

import os
import json
import urllib2
import csv
from datetime import datetime, date, timedelta
  
  
def get_hiev_csv(filename):
  '''
  Function that downloads and returns a csv-reader object of the most recent facilities 
  or experiments info file from HIEv 
  '''
  request_url = 'https://hiev.uws.edu.au/data_files/api_search'
  api_token = os.environ['HIEV_API_KEY']
  request_headers = {'Content-Type' : 'application/json; charset=UTF-8', 'X-Accept': 'application/json'}
  request_data = json.dumps({'auth_token': api_token, 'filename': filename})
  # --Handle the returned response from the HIEv server
  request  = urllib2.Request(request_url, request_data, request_headers)
  response = urllib2.urlopen(request)
  js = json.load(response)
  # Grab the latest - in those cases where there are multiple resukts returned
  js_latest = (sorted(js, key=lambda k: k['updated_at'], reverse=True))[0]
  download_url = js_latest['url']+'?'+'auth_token=%s' %api_token
  request = urllib2.Request(download_url)
  f = urllib2.urlopen(request)
  return csv.reader(f, delimiter=',', quotechar='"', skipinitialspace=True)
    

def match_count(match_dict, jsondata):
  ''' 
  Function that recieves a key to match, the value to match it against and returns the number of 
  matching entries found within the supplied json data 
  '''
  count = 0
  #loop through each record of the json  
  for record in jsondata:
    #check all key value pairs against the current record and add to count if all are present    
    matched = True
    for key in match_dict:
      if record[key]:
        if str(record[key]) != str(match_dict[key]):
          matched = False
          break
      else:
        matched = False
    count += matched
  
  return count;


## MAIN PROGRAM

# -- Set global http call values
request_url = 'https://hiev.uws.edu.au/data_files/api_search'
api_token = os.environ['HIEV_API_KEY']
request_headers = {'Content-Type' : 'application/json; charset=UTF-8', 'X-Accept': 'application/json'}

# -- Get listing of all HIEv files
request_data = json.dumps({'auth_token': api_token})
request  = urllib2.Request(request_url, request_data, request_headers)
response = urllib2.urlopen(request)
js_all = json.load(response)
# -- Get listing of HIEv files uploaded in the last day
upload_from_date = str(date.today() - timedelta(days=1))
upload_to_date = str(date.today()- timedelta(days=0))
request_data = json.dumps({'auth_token': api_token, 'upload_from_date': upload_from_date, 'upload_to_date': upload_to_date})
request  = urllib2.Request(request_url, request_data, request_headers)
response = urllib2.urlopen(request)
js_lastday = json.load(response)

# -- Begin writing the stats dictionary
hiev_stats = {}

# root element
hiev_stats['hiev_stats'] = {}

# total files
hiev_stats['hiev_stats']['total_files'] = len(js_all)

# files uploaded in the last day
hiev_stats['hiev_stats']['last_day_files'] = len(js_lastday)

# files by type
types = hiev_stats['hiev_stats']['types'] = []
for type in ['RAW', 'PROCESSED', 'CLEANSED', 'ERROR']:    
    type_record = {}
    type_record['type'] = type
    type_record['total_files']=match_count({'file_processing_status':type}, js_all)
    type_record['last_day_files']=match_count({'file_processing_status':type}, js_lastday)
    types.append(type_record)

# begin facilities nested section, first reading in the facility and experiment information from the most recent HIEv files
# convert each to a list - makes it easier to reiterate over
facilities_csv = list(get_hiev_csv('HIEv_Facilities_List_'))[1:] 
experiments_csv = list(get_hiev_csv('HIEv_Experiments_List_'))[1:]

facilities = hiev_stats['hiev_stats']['facilities'] = []

for facrow in facilities_csv:
    fac_record={}
    fac_record['id'] = facrow[0]
    fac_record['name'] = facrow[1]
    #get a count of the total number of files uploaded for this facility
    fac_record['total_files'] = match_count({'facility_id': str(facrow[0])}, js_all)
    #get a count of the number of files uploaded in the last 24 hours for this facility
    fac_record['last_day_files'] = match_count({'facility_id': str(facrow[0])}, js_lastday)
    # finally append the facility record to the full facilities list 
    facilities.append(fac_record)
    
    # Type by facility - For each facility begin a 'type' nested section
    types = fac_record['Types'] = []
    for type in ['RAW', 'PROCESSED', 'CLEANSED', 'ERROR']:    
        type_record = {}
        type_record['type'] = type
        type_record['total_files']=match_count({'facility_id': str(facrow[0]), 'file_processing_status':type}, js_all)
        type_record['last_day_files']=match_count({'facility_id': str(facrow[0]), 'file_processing_status':type}, js_lastday)
        types.append(type_record)
        
    # For each facility begin an experiments nested section, reading in experiment information from the most recent HIEv file
    experiments = fac_record['experiments'] = []
    # Loop over experiment csv and pull out any lines that match the current facility ID
    for exprow in experiments_csv:
        if exprow[1] == facrow[0]:
            exp_record={}
            #add the ID and name of the matching experiment  
            exp_record['id'] = exprow[0]
            exp_record['name'] = exprow[2]
            #get a count of the total number of files uploaded for this experiment
            exp_record['total_files'] = match_count({'experiment_id':str(exprow[0])}, js_all)
            #get a count of the number of files uploaded in the last 24 hours for this experiment
            exp_record['last_day_files'] = match_count({'experiment_id':str(exprow[0])}, js_lastday)
            # finally append the facility record to the full facilities list 
            experiments.append(exp_record)


# -- Write the data to json file

with open('hiev_stats.json', 'w') as f:
    json.dump(hiev_stats, f, sort_keys = True, indent = 4, ensure_ascii=False)

f.close()
