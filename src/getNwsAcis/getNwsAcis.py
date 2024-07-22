#!/bin/env python3
# this script will download precip and temp data from NWS ACIS
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


parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-d", "--days_back", default=7,
                      help="""Days back from current time to get data.  Can be decimal and integer values"""
                    )
parser.add_argument("-o", "--hour_offset", default="12", help="Offset UTC hour to store daily data")
args = vars(parser.parse_args())

days_back = float(args["days_back"])
hour_offset = float(args["hour_offset"])

# grab API variables from .env file
load_dotenv()
APIROOT = os.getenv("API_ROOT")
OFFICE = os.getenv("OFFICE")
APIKEY = os.getenv('API_KEY')
# connect to T7
apiKey = "apikey " + APIKEY
api = cwms.api.init_session(api_root=APIROOT, api_key=apiKey)

# acis elements
elems = [
    {"name":"pcpn"}, 
    {"name":"snow"}, 
    {"name":"snwd"}, 
    {"name":"maxt"}, 
    {"name":"mint"}, 
    {"name":"obst"},
    
]
#ts version to save as
version = 'Raw-NWS-ACIS'
# ts element to tsid mapping
tsMapping = {"pcpn":f"Precip.Total.~1Day.1Day.{version}", 
    "snow":f"Depth-Inc-Snow.Total.~1Day.1Day.{version}", 
    "snwd":f"Depth-Snow.Total.~1Week.1Month.{version}", 
    "maxt":f"Temp-Air.Max.~1Day.1Day.{version}", 
    "mint":f"Temp-Air.Min.~1Day.1Day.{version}", 
    "obst":f"Temp-Air.Inst.~1Day.0.{version}"}
#unit mapping  
unitMapping = {"pcpn":"in", 
    "snow":"in", 
    "snwd":"in", 
    "maxt":"F", 
    "mint":"F", 
    "obst":"F"}
#missing value in CWMS
missingValueCwms = -340282346638528859811704183484516925440

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
url = 'https://data.rcc-acis.org/StnData'
#logger.info(
#    f"Data will be grabbed and stored from NWS ACIS from {url} repo")

def main():

    execution_date = datetime.now()

    # create list of column names
    cols =  [elem["name"] for elem in elems]
    # add date-time as column
    cols.insert(0, 'date-time')

    # build start date and end date
    sdate = (datetime.now()-timedelta(days=days_back)).strftime("%Y-%m-%d")
    edate = datetime.now().strftime("%Y-%m-%d")
    logger.info(f'Days to look back {days_back}')
    logger.info(f'Will store at offset time {hour_offset}')
    logger.info(f'Getting data from {sdate} through {edate}')

    # get CWMS locations in MVP NWS ACIS Project group...this triggers what data to get
    df_loc_data = cwms.get_location_group(category_id="NWS ACIS",loc_group_id="Projects",office_id=OFFICE).df
    # get NWS aliases
    df_loc_nws = cwms.get_location_group(category_id="Agency Aliases", loc_group_id="NWS Handbook 5 ID",office_id="CWMS").df

    # merge CWMS locations in ACIS project group with NWS aliases
    nws_alias = pd.merge(df_loc_data, df_loc_nws, on='location-id').set_index('location-id')
    # list to store errors
    storErr = []

    # walk through locations
    for index, row in nws_alias.iterrows():
        loc = index
        nws_id = row['alias-id']
        logger.info(f'Attempting to retrieve data from {nws_id} and store in {loc}')
        input_dict = {
            "sid":nws_id,
            "sdate":sdate,
            "edate":edate,
            "elems":elems,
        }
        params = {'params': json.dumps(input_dict)}
        headers = {'Accept': 'application/json'}
        # get data
        req = requests.post(url, data=params, headers=headers)
        response = req.json()
        #print(response)
        try:
            acis_data = response['data']
            # convert to pandas dataframe
            df = pd.DataFrame(acis_data, columns = cols)
            #standardize to typical entry time
            df["date-time"] = pd.to_datetime(df["date-time"])+ pd.Timedelta(hours=hour_offset)
            # walk through each of the parameter columns, skipping date-time column
            for param in cols[1:-1]:
                
                #get units from unit mapping dict
                units = unitMapping[param]
                # create subset of dataframe with just one parameter
                d = df[['date-time', param ]].copy()
                # set quality code
                d.loc[:,'quality-code'] = 0
                # rename columns
                d.columns = ['date-time', 'value', 'quality-code']
                #drop missing values
                d = d[d['value'] != 'M']
                #change trace flag to .001
                d['value'] = d['value'].replace('T', str(0.001))
                #change S flag to missing
                d['value'] = d['value'].replace('S', str(missingValueCwms))
                # Screen out A flag which is paired w/ S flag to signify a multiday total
                d['value'] = d['value'].replace(r'[^0-9\.]', '', regex=True)
                # assign timeseries id from mapping dictionary
                tsId = f'{loc}.{tsMapping[param]}'
                #convert to json
                data_json = cwms.timeseries_df_to_json(data = d, 
                                                tsId = tsId, units = units, office_id = OFFICE)
                #print(data_json)
                #store data
                cwms.store_timeseries(data=data_json)
        except:
            logger.info(f'error retrieving data from {loc}')  
            storErr.append(loc)

    logger.info(f"The following locs errored when retrieving or storing {storErr}")
   


if __name__ == "__main__":
    main()