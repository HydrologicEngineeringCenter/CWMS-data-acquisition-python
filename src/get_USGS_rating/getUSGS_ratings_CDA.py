#!/bin/env python3
# This getUSGS script works with CDA version 20250305.
# and cwms-python version 0.6

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from json import loads
import cwms
from dataretrieval import nwis
import requests
import os
import sys
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-d", "--days_back", default="1", help="Days back from current time to check for updated ratings.  Can be decimal and integer values")
parser.add_argument("-o", "--office", required=True, type=str, help="Office to grab data for (Required).")
parser.add_argument("-a", "--api_root", required=True, type=str, help="Api Root for CDA (Required).")
parser.add_argument("-k", "--api_key", default=None, type=str, help="api key. one of api_key or api_key_loc are required")
parser.add_argument("-kl", "--api_key_loc", default=None, type=str, help="file storing Api Key. One of api_key or api_key_loc are required")
args = vars(parser.parse_args())

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

OFFICE = args["office"]
APIROOT = args["api_root"]
# place a file in .cwms names api_key that holds you apikey
# to be used to write data to the database using CDA
if args["api_key_loc"] is not None:
    api_key_loc = args["api_key_loc"]
    with open(api_key_loc, "r") as f:
        APIKEY = f.readline().strip()
elif args["api_key"] is not None:
    APIKEY=args["api_key"]
else:
    raise Exception("must add a value to either --api_key(-a) or --api_key_loc(-al)") 

# Days back is defined as a argument to the program.
# run program by typing python3 getUSGS.py 5
# the 5 would mean grab data starting 5 days ago to now
DAYS_BACK = float(args["days_back"])

# import CWMS module and assign the apiROOT and apikey to be
# used throughout the program
apiKey = "apikey " + APIKEY
api = cwms.api.init_session(api_root=APIROOT, api_key=apiKey)

def get_rating_ids_from_specs(office_id):
    rating_types = ['EXSA', 'CORR', 'BASE']
    rating_specs = cwms.get_rating_specs(office_id=office_id).df
    if 'effective-dates' not in rating_specs.columns: rating_specs['effective-dates'] = np.nan
    rating_specs = rating_specs.dropna(subset=['description'])
    for rating_type in rating_types:
        rating_specs.loc[rating_specs['description'].str.contains(f'USGS-{rating_type}'), 'rating-type'] = rating_type
    rating_specs = rating_specs[(
        rating_specs['rating-type'].isin(rating_types)) & 
        (rating_specs['active']==True) & 
        (rating_specs['auto-update']==True)]
    return rating_specs

def get_location_aliases(df, loc_group_id, category_id, office_id,category_office_id,group_office_id):
    #CDA get location group endpoint has an error with category and group office ids.  need to update when error is fixed.
    Locdf = cwms.get_location_group(loc_group_id=loc_group_id,
                                    category_id=category_id,
                                    office_id=office_id,
                                    category_office_id=category_office_id,
                                    group_office_id=group_office_id).df
    USGS_alias = Locdf[Locdf['alias-id'].notnull()]
    USGS_alias = USGS_alias.rename(columns = {'alias-id': 'USGS_St_Num','attribute':'Loc_attribute'})
    USGS_alias.USGS_St_Num = USGS_alias.USGS_St_Num.str.rjust(8,'0')
    USGS_ratings = pd.merge(df, USGS_alias,  how='inner', on=['location-id','office-id'])
    return USGS_ratings

def get_usgs_updated_ratings(period):
    '''
    Function to grab data from the USGS based off of dataretieve-python
    ''' 
    # Get USGS data
    base_url = "https://nwis.waterdata.usgs.gov/nwisweb/get_ratings"

    query_dict = {
        "period":period,
        "format": "rdb"
    }

    r = requests.get(base_url, params = query_dict)
    temp = pd.DataFrame(r.text.split('\n'))
    temp = temp[temp[0].str.startswith("USGS")]
    updated_ratings = temp[0].str.split("\t",expand=True)
    updated_ratings.columns = ["org","USGS_St_Num","rating-type","date_updated","url"]
    updated_ratings["rating-type"] = updated_ratings["rating-type"].str.upper()
    return updated_ratings

def convert_tz(tz:str):
    if tz in ("AST","ADT"): tzid = "America/Halifax"
    elif tz in ("EST","EDT"): tzid = "US/Eastern"
    elif tz in ("CST","CDT"): tzid = "US/Central"
    elif tz in ("MST","MDT"): tzid = "US/Mountain"
    elif tz in ("PST","PDT"): tzid = "US/Pacific"
    elif tz in ("AKST","AKDT"): tzid = "America/Anchorage"
    elif tz in ("UTC","GMT"): tzid = "UTC"
    else: tzid = tz
    return tzid

def get_usgs_tz(data):
    line = data[data[0].str.startswith("# //STATION AGENCY=")].iloc[0,0]
    timezone = line.split("TIME_ZONE=")[1].split()[0].replace('"', '')
    timezone = convert_tz(timezone)
    return timezone

def get_begin_with_date(data,str_starts):
    date_string = None
    lines = data[data[0].str.startswith(str_starts)]
    for _, line in lines.iterrows():
        timestr = line[0].split("BEGIN=")[1].split()[0].strip().replace('"', '')
        if timestr.isdigit(): date_string = timestr
    return date_string

def get_usgs_effective_date(data,rating_type):

    date_string = None
    if rating_type == 'EXSA':
        line = data[data[0].str.startswith("# //RATING SHIFTED=")].iloc[0,0]  
        rating_shifted_date = line.split('=')[1].replace('"', '')
        date_string = rating_shifted_date.split()[0]

    elif rating_type == 'BASE':
        date_string = get_begin_with_date(data, ("# //RATING_DATETIME BEGIN="))

    elif rating_type == 'CORR':
        date_string = get_begin_with_date(data, ("# //CORR1_PREV BEGIN=","# //CORR2_PREV BEGIN=","# //CORR3_PREV BEGIN="))
        
    if date_string is None:
        line = data[data[0].str.startswith("# //RETRIEVED:")].iloc[0,0]
        date_string = line.split('RETRIEVED: ')[1]
        
    timezone = get_usgs_tz(data)   
    dt = pd.to_datetime(date_string).tz_localize(timezone).floor('Min')
    return dt

def convert_usgs_rating_df(df, rating_type):
    if rating_type == 'CORR':
        df = df.groupby("CORR")
        df = pd.concat([df.first(),df.last()], ignore_index=True, join="inner")
        df = df.sort_values(by=['INDEP'],ignore_index=True)
    df = df.rename(columns={"INDEP": "ind", "CORRINDEP": "dep", "DEP":"dep"})
    df_out = df[['ind', 'dep']].copy()
    return df_out

def cwms_write_ratings(updated_ratings):
    storErr = []
    usgsapiErr = []
    usgsemptyErr = []
    usgseffectiveErr = []
    total_recs = len(updated_ratings.index)
    saved = 0
    same_effective = 0

    rating_units={'EXSA':'ft;cfs','BASE':'ft;cfs','CORR':'ft;ft'}
    for _, row in updated_ratings.iterrows():
        logger.info(f'Getting data for rating ID = {row["rating-id"]}')
        logger.info(f'Getting data from USGS for USGS ID = {row["USGS_St_Num"]}, Rating Type = {row["rating-type"]}')
        try:
            usgs_rating, meta = nwis.get_ratings(site=row['USGS_St_Num'], file_type=str(row['rating-type']).lower())
            url = meta.url
        except Exception as error:
            usgsapiErr.append([row['rating-id'], row["USGS_St_Num"], row["rating-type"], error])
            logger.error(
                    f'FAIL Error collecting rating data from USGS for -->  {row["rating-id"]},{row["USGS_St_Num"]}, {row["rating-type"]} USGS error = {error}'
                )
            continue
        if usgs_rating.empty:
            logger.warning(f'Empty rating obtained from USGS for USGS ID = {row["USGS_St_Num"]}, Rating Type = {row["rating-type"]}, url')
            usgsemptyErr.append([row['rating-id'], row["USGS_St_Num"], row["rating-type"]])
        else: 
            try:
                response = requests.get(url)
                temp = pd.DataFrame(response.text.split('\n'))
                usgs_effective_date = get_usgs_effective_date(temp,row['rating-type'])
            except Exception as error:
                usgseffectiveErr.append([row['rating-id'], row["USGS_St_Num"], row["rating-type"], error])
                logger.error(
                        f'FAIL Error collecting effective date from USGS rating -->  {row["rating-id"]},{row["USGS_St_Num"]}, {row["rating-type"]} CDA error = {error}'
                    )
                continue
            cwms_effective_date = row['cwms_max_effective_date']
            logger.info(f"Effective dates: cwms = {cwms_effective_date}, usgs = {usgs_effective_date}")
            if (cwms_effective_date == usgs_effective_date) or (cwms_effective_date == (usgs_effective_date + timedelta(hours=1))):
                logger.info("Effective dates are the same rating curve will not be saved")
                same_effective = same_effective + 1
            else:
                try:
                    usgs_store_rating = convert_usgs_rating_df(usgs_rating,row['rating-type'])
                    
                    if row['effective-dates'] and row['auto-migrate-extension']:
                        current_rating = cwms.get_ratings(
                                                rating_id=row['rating-id'],
                                                office_id=row['office-id'],
                                                begin=cwms_effective_date,
                                                end=cwms_effective_date,
                                                method="EAGER",
                                                single_rating_df=True)
                        rating_json = current_rating.json
                        points_json = loads(usgs_store_rating.to_json(orient="records"))
                        rating_json["simple-rating"]["rating-points"] = {"point": points_json}
                        rating_json["simple-rating"]["effective-date"] = usgs_effective_date.isoformat()
                        del rating_json["simple-rating"]["create-date"]
                        rating_json["simple-rating"]["active"] = row['auto-activate']
                    else:
                        rating_json = cwms.rating_simple_df_to_json(data=usgs_store_rating,rating_id=row['rating-id'],office_id=row['office-id'],units=rating_units[row['rating-type']],effective_date=usgs_effective_date,active=row['auto-activate'])
                    response = cwms.update_ratings(data = rating_json, rating_id = row['rating-id'])
                    logger.info(f'SUCCESS Stored rating for rating id = {row["rating-id"]}, effective date = {usgs_effective_date}')
                    saved = saved + 1
                except Exception as error:
                    storErr.append([row['rating-id'], row["USGS_St_Num"], row["rating-type"], error])
                    logger.error(
                            f'FAIL Data could not be stored to CWMS database for -->  {row["rating-id"]},{row["USGS_St_Num"]}, {row["rating-type"]} CDA error = {error}'
                        )
    logger.info(f"A total of {same_effective + saved} out of {total_recs} records were successfully saved or had same effective date in cwms")
    if len(usgsapiErr) > 0: logger.info(f"The following ratings errored out when accessing the USGS API: {usgsapiErr}")
    if len(usgsemptyErr) > 0: logger.info(f"The following ratings had an empty rating curve returned from the usgs: {usgsemptyErr}")
    if len(usgseffectiveErr) > 0: logger.info(f"The following ratings errored when trying to determine the effective date from the USGS: {usgseffectiveErr}")
    if len(storErr) > 0:  logger.info(f"The following ratings errored when trying to store to CDA: {storErr}")

def main():
    logger.info(f"CDA connection: {APIROOT}")
    logger.info(
        f"Updated Ratings will be check from the USGS for the past {DAYS_BACK} days")
    execution_date = datetime.now()
    logger.info(f"Execution date {execution_date}")
    
    logger.info(f"Get Rating Spec information from CWMS Database")
    rating_specs = get_rating_ids_from_specs(OFFICE)
    USGS_ratings = get_location_aliases(rating_specs,"USGS Station Number","Agency Aliases","CWMS",None,None)
    
    # grab ratings that don't have an existing rating curve.  ie new specs.
    USGS_ratings_empty = USGS_ratings[USGS_ratings['effective-dates'].isna()]
    USGS_ratings = USGS_ratings[USGS_ratings['effective-dates'].notna()]
    
    logger.info(f"Get list of ratings updated by USGS in past {DAYS_BACK} days")
    df = get_usgs_updated_ratings(DAYS_BACK*24)

    updated_ratings = pd.merge(USGS_ratings, df, how='inner', left_on=['USGS_St_Num','rating-type'],right_on=['USGS_St_Num','rating-type'])

    updated_ratings.loc[:,'effective-dates'] = updated_ratings['effective-dates'].apply(lambda x: [pd.to_datetime(d) for d in x])
    updated_ratings.loc[:,'cwms_max_effective_date'] = updated_ratings['effective-dates'].apply(max)

    #merge the new specs without an existing curve back into the update ratings df
    if not USGS_ratings_empty.empty:
        updated_ratings = pd.concat([updated_ratings, USGS_ratings_empty],ignore_index=True)

    cwms_write_ratings(updated_ratings)
           
if __name__ == "__main__":
    main()