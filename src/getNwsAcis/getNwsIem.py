#!/bin/env python3
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import cwms
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
# load .env python environment for storing API_KEY
# .env file can be stored a parent directory of script
# https://dev.to/jakewitcher/using-env-files-for-environment-variables-in-python-applications-55a1
from dotenv import load_dotenv
import os
import requests
import json

def find_first_value(list_of_dicts, key, timestamp):
    for dictionary in list_of_dicts:
        #print(key,  dictionary[key])
        if key in dictionary and dictionary[key]!= None:
            return dictionary[timestamp], dictionary[key]
    return None, None  # Return None if the key is not found in any dictionary

parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-d", "--days_back", default=10,
                      help="""Days back from current time to get data.  Can be decimal and integer values"""
                    )
args = vars(parser.parse_args())

# days back to get data
days_back = int(args["days_back"])

# grab API variables from .env file
load_dotenv()
APIROOT = os.getenv("API_ROOT")
OFFICE = os.getenv("OFFICE")
APIKEY = os.getenv('API_KEY')
# connect to T7
apiKey = "apikey " + APIKEY
api = cwms.api.init_session(api_root=APIROOT, api_key=apiKey)




#url
url = 'https://mesonet.agron.iastate.edu/api/1/obhistory.json?station={}&network={}&date={}&full=1'
#station link to get states
sta_index_link = 'https://mesonet.agron.iastate.edu/sites/networks.php?network=_ALL_&format=csv&nohtml=on'


#ts version to save as
version = 'Raw-NWS-IEM'
# SHEF to ts_id mapping
tsMapping = {
    "GDIRZZZ":f"Depth-Frost.Inst.~1Week.0.{version}", 
    "GTIRZZZ":f"Depth-Frost-Thawed.Inst.~1Week.0.{version}", 
    "ICIRZZZ":f"%-Ice.Inst.~1Week.0.{version}", 
    "ITIRZZZ":f"Depth-Ice.Inst.~1Week.0.{version}", 
    "UDIRZZZ":f"Dir-Wind.Inst.~1Day.0.{version}", 
    "USIRZZZ":f"Speed-Wind.Inst.~1Day.0.{version}", 
    "TWIRZZZ":f"Temp-Water.Inst.~1Day.0.{version}",
}
#unit mapping  
unitMapping = {"Depth":"in", 
    "Dir-Wind":"deg", 
    "%-Ice":"%", 
    "Speed-Wind":"mph", 
    "Temp-Water":"F"
              }
#missing value in CWMS
cwms_missing_value = -340282346638528859811704183484516925440
cwms_missing_quality = 5


# create logger for logging
logger = logging.getLogger()
if (logger.hasHandlers()):
    logger.handlers.clear()
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False


#logger.info(f"CDA connection: {APIROOT}")
    #acis url

#logger.info(
#    f"Data will be grabbed and stored from NWS IEM from {url} repo")

def main():

    execution_date = datetime.now()
    # Build start date and end date
    edate = datetime.now()
    sdate = edate - timedelta(days=days_back)

    # Create a list of dates using a for loop
    date_list = []
    for i in range(days_back + 1):  # +1 to include both start and end dates
        current_date = sdate + timedelta(days=i)
        date_list.append(current_date.strftime("%Y-%m-%d"))
    
    # get CWMS locations in MVP NWS ACIS Project group...this triggers what data to get
    df_loc_data = cwms.get_location_group(category_id="NWS ACIS",loc_group_id="Projects",office_id=OFFICE).df
    # get NWS aliases
    df_loc_nws = cwms.get_location_group(category_id="Agency Aliases", loc_group_id="NWS Handbook 5 ID",office_id="CWMS").df
    
    # merge CWMS locations in ACIS project group with NWS aliases
    nws_alias = pd.merge(df_loc_data, df_loc_nws, on='location-id').set_index('location-id')


    #grab the station list from IEA to link the provience (easier than trying to get it from the db location table)
    sta_index = pd.read_csv(sta_index_link)
    sta_index = sta_index.rename(columns={'stid': 'alias-id'})
    sta_index = sta_index[['alias-id','iem_network']]

    #link the network to the location group 
    nws_alias = pd.merge(sta_index, nws_alias.reset_index(), on='alias-id')
    #eccc_alias.index = eccc_alias['location-id']
    nws_alias.index = nws_alias['location-id']   

    # walk through locations
    for index, row in nws_alias.iterrows():
        loc = index
        #if loc == 'LacQuiParle_Dam':
        nws_id = row['alias-id']
        network = row['iem_network'].split('_')[0]+'_COOP'
        
        print(loc, nws_id, network)

        for date in date_list:

            print(date)
            requestUrl = url.format(nws_id, network, date)
            print(requestUrl)
            # get data
            req = requests.get(requestUrl)
            response = req.json()
            #print(response)
            #try:
            if 'data' not in response:
                break
            try:
                # loop through data dicts
                dataList = response['data']
                if len(dataList)>0:         
                    # loop through parameters and grab first non-none
                    for k, v in tsMapping.items():
                        #print(k,v)
                        t, value = find_first_value(dataList, k, 'utc_valid')
                        #print(t,value)
                        if t:
                            dt = datetime.strptime(t, '%Y-%m-%dT%H:%M%z')
                            ts_id = loc+'.'+v
                            for param, u in unitMapping.items():
                                if param in ts_id:
                                    units = u
                            try:
                                #value = data[k]
                            
                                df = pd.DataFrame([[dt, value, 0]], columns = ['date-time', 'value', 'quality-code'])
                                #convert to json
                                data_json = cwms.timeseries_df_to_json(data = df, 
                                                                        ts_id = ts_id, units = units, office_id = OFFICE)
                                cwms.store_timeseries(data=data_json)

                                logger.info(f'success storing {ts_id}')
                            except:

                                logger.info(f'failed getting {ts_id}')
            except:
                logger.info(f'failed getting data for {loc} on {date}')  


if __name__ == "__main__":
    main()