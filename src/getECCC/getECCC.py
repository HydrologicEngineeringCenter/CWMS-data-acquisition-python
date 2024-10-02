#!/bin/env python3
# this script will download flow and stage instantaneous data from Environment Canada
# https://eccc-msc.github.io/open-data/msc-data/obs_hydrometric/readme_hydrometric-datamart_en/ 
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


parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-f", "--eccc_frequency", default="hourly", choices=["hourly", "daily"],  
                    help="""Retrieve from instantaneous data repo that goes back 2 days and is updated hourly,
                      or instantaneous data repo that goes back 1 month but is updated once during the night."""
                    )
args = vars(parser.parse_args())

# grab API variables from .env file
load_dotenv()
APIROOT = os.getenv("API_ROOT")
OFFICE = os.getenv("OFFICE")
APIKEY = os.getenv('API_KEY')

#ECCC station list to link provence's to station
STA_INDEX_LINK = 'https://dd.weather.gc.ca/hydrometric/doc/hydrometric_StationList.csv'

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

# run program by typing python3 geECCC.py hourly or geECCC.py daily
# the hourly would mean grab data that goes back 2 days is updated hourly
# daily would mean grab data that goes back 1 month and is updated daily
ECCC_FREQ = args["eccc_frequency"]

# import CWMS module and assign the apiROOT and apikey to be
# used throughout the program
apiKey = "apikey " + APIKEY
api = cwms.api.init_session(api_root=APIROOT, api_key=apiKey)
print(api)

def get_CMWS_TS_Loc_Data(office):
    """
    get time series group and location alias information and combine into singe dataframe

    """
    # get ECCC timeseries group
    df = cwms.get_timeseries_group(group_id="ECCC TS Data Acquisition",category_id="Data Acquisition",office_id="CWMS").df
    # parse out the time seried id into components
    df[['location-id','param','type','int','dur','ver']] = df['timeseries-id'].str.split('.', expand=True)

    # get cwms location group to link ECCC station ID
    Locdf = cwms.get_location_group(loc_group_id = "ECCC Station ID", category_id = "Agency Aliases",
                                    office_id = "CWMS",).df.set_index('location-id')

    #Grab all of the locations that have a eccc station number assigned to them
    eccc_alias=Locdf[Locdf['alias-id'].notnull()]

    #rename the columns
    eccc_alias = eccc_alias.rename(columns = {'alias-id': 'ECCC_St_ID','attribute':'Loc_attribute'})
    eccc_alias = eccc_alias.reset_index()

    #grab the station list from ECCC to link the provience (easier than trying to get it from the db location table)
    sta_index = pd.read_csv(STA_INDEX_LINK)
    sta_index = sta_index.rename(columns={'ID': 'ECCC_St_ID'})
    sta_index = sta_index[['ECCC_St_ID','Prov/Terr']]

    #link the province to the eccc location group 
    sta_index.columns
    eccc_alias = pd.merge(eccc_alias, sta_index, on='ECCC_St_ID')
    eccc_alias.index = eccc_alias['location-id']
   
    # do an inner join with the time series that are in the ECCC time series group and the location group.  Join based on the Location ID and office if
    eccc_ts = df.join(eccc_alias,on='location-id',lsuffix='ts',rsuffix='loc',how='inner')



    ## join the timeseries group with the location group
    #eccc_ts = df.join(eccc_alias, on='location-id',lsuffix='ts',rsuffix='loc',how='inner')

    logger.info(f"CWMS TS Groups and Location Data Obtained")
    return eccc_ts
def check_even_5min_interval(df, datetime_col):
    """Checks if a datetime column is on an even 5-minute interval."""
    tmpDf = df.copy()
    
    tmpDf[datetime_col] = pd.to_datetime(tmpDf[datetime_col])

    tmpDf['time_diff'] = tmpDf[datetime_col].diff()
    tmpDf['time_diff_minutes'] = tmpDf['time_diff'].dt.total_seconds() / 60

    # Check if all time differences are multiples of 5
    return (tmpDf['time_diff_minutes'] % 5 == 0).all()

def check_and_shift_to_5min(df, datetime_col):
    """
    Checks if a datetime column is on a 5-minute interval and shifts data if not.

    Args:
        df (pd.DataFrame): The DataFrame containing the datetime column.
        datetime_col (str): The name of the datetime column.

    Returns:
        pd.DataFrame: The DataFrame with the datetime column on a 5-minute interval.
    """
    tmpDf = df.copy()

    # Convert the column to datetime if it's not already
    tmpDf[datetime_col] = pd.to_datetime(tmpDf[datetime_col])


    is_5min_interval = check_even_5min_interval(tmpDf.copy(), datetime_col)

    if not is_5min_interval:
        # Round the datetime column to the nearest 5 minutes
        tmpDf[datetime_col] = tmpDf[datetime_col].dt.round('5min')
    
    #tmpDf.dropna(subset=['value'], inplace=True)

    return tmpDf

def getECCC_data(prov, id, ECCC_FREQ, snapTo5Minutes = True) -> pd.DataFrame:

    url = 'https://dd.weather.gc.ca/hydrometric/csv/'+prov+'/'+ECCC_FREQ+'/'+prov+'_'+id+'_'+ECCC_FREQ+'_hydrometric.csv'
    logger.info(url)
    try:
        df = pd.read_csv(url)
    except:
        logger.info(url+' failed')
        df = pd.DataFrame()

    if df.empty==False:
        df['Date']= pd.to_datetime(df['Date'])
        # need to convert from '2024-02-16 08:30:00-06:00' format to '2024-02-16 14:30:00' so it stores in CWMS correctly
        df['Date'] = df['Date'].values 
        # will snap to the nearest 5 minutes if true
        if snapTo5Minutes:
            df = check_and_shift_to_5min(df, 'Date')
        dfStage = df[["Date", "Water Level / Niveau d'eau (m)"]]
        dfStage.columns = ['date', 'value']
        #drop any na values
        dfStage = dfStage.dropna()
        dfFlow = df[["Date", "Discharge / Débit (cms)"]]
        dfFlow.columns = ['date', 'value']
        #drop any na values
        dfFlow = dfFlow.dropna()
        logger.info(f"Data obtained from ECCC")
        return dfStage, dfFlow
    else:
           return None, None 


def CWMS_writeData(df, ts_id, units, OFFICE, qualityCode):
    values = df.reindex(columns=['date','value'])
    #adjust column names to fit cwms-python format.
    values = values.rename(columns={'date': 'date-time', 'value': 'value'})
    values['quality-code'] = qualityCode 
    data = cwms.timeseries_df_to_json(data = values, ts_id = ts_id, units = units, office_id = OFFICE)
    
    #write values to CWMS database
    x = cwms.store_timeseries(data=data)
    logger.info(f"Data stored in CWMS ECCC")
    return(x)


def loopThroughTs(ECCC_ts):
    # list to hold time series that fail
    storErr = []
    total_recs = len(ECCC_ts.index)
    lastStation = None
    saved = 0

    for index, row in ECCC_ts.iterrows():
        eccc_id = row.ECCC_St_ID
        prov = row['Prov/Terr']
        ts_id = row['timeseries-id']
        # if the station is different from the last one
        if eccc_id != lastStation:
            #logger.info(f"Attempting to write values for ts_id -->  {eccc_id}")
            # get the data
            dfStage, dfFlow = getECCC_data(prov, eccc_id, ECCC_FREQ)
        if row.param == 'Flow':
            # if flow parameter and there are values, store them
            if isinstance(dfStage, pd.DataFrame) and dfFlow.value.isnull().all() == False:
                logger.info(f"Attempting to write values for ts_id -->  {eccc_id} {ts_id}")
                try:
                    CWMS_writeData(dfFlow, ts_id, 'cms', OFFICE, 0)
                    saved = saved + 1
                except:
                    storErr.append(row['timeseries-id'])
                    logger.info(f"Error writing values for ts_id -->  {eccc_id} {ts_id}")
        if row.param == 'Stage' or row.param == 'Elev':
            # else store the stage or elevation values
            if isinstance(dfStage, pd.DataFrame) and dfStage.value.isnull().all() == False:
                logger.info(f"Attempting to write values for ts_id -->  {eccc_id} {ts_id}")
                try:
                    CWMS_writeData(dfStage, ts_id, 'm', OFFICE, 0)
                    saved = saved + 1
                except:
                    storErr.append(row['timeseries-id'])
                    logger.info(f"Error writing values for ts_id -->  {eccc_id} {ts_id}")
        lastStation = eccc_id


    logger.info(f"A total of {saved} records were successfully saved out of {total_recs}")
    logger.info(f"The following ts_ids errored when storing {storErr}")


def main():
    logger.info(f"CDA connection: {APIROOT}")
    logger.info(
        f"Data will be grabbed and stored from ECCC from {ECCC_FREQ} repo")
    execution_date = datetime.now()

    # grab all of the unique USGS stations numbers to be sent to USGS api
    ECCC_ts = get_CMWS_TS_Loc_Data(OFFICE)

    logger.info(f"Execution date {execution_date}")

    logger.info(f"Grabing data from ECCC")
    

    loopThroughTs(ECCC_ts)


if __name__ == "__main__":
    main()

