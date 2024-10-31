## Call Necessary Libraries
import requests
import gzip
import io
import logging
import json
import cwms
import datetime
import pytz
import sys
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from getpass import getpass
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

## Set Up Parser Arguments
parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument("-o", "--office", required=True, type=str, help="Office to grab data for (Required).")
parser.add_argument("-a", "--api_root", required=True, type=str, help="Api Root for CDA (Required).")
parser.add_argument("-k", "--api_key", default=None, type=str, help="api key. one of api_key or api_key_loc are required")
parser.add_argument("-kl", "--api_key_loc", default=None, type=str, help="file storing Api Key. One of api_key or api_key_loc are required")
args = vars(parser.parse_args())

## Create Logger for Logging
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
storErr = []

## Set Up the API
OFFICE = args["office"]
APIROOT = args["api_root"]

if args["api_key_loc"] is not None:
    api_key_loc = args["api_key_loc"]
    with open(api_key_loc, "r") as f:
        APIKEY = f.readline().strip()
elif args["api_key"] is not None:
    APIKEY=args["api_key"]
else:
    raise Exception("must add a value to either --api_key(-a) or --api_key_loc(-al)")
apiKey = "apikey " + APIKEY
api = cwms.api.init_session(api_root=APIROOT, api_key=apiKey)

## Define Necessary Functions/Arguments
# Missing Values in CWMS
Missing_Value = -340282346638528859811704183484516925440
Missing_Quality_Code = 5

#Goes to https://tgftp.nws.noaa.gov/data/rfc/lmrfc/misc/ and Gets the NAEFS 28-Day PI XML Forecast
def get_NAEFS_PIXML():
    logger.info(f"Searching for Most Recent LMRFC NAEFS Forecast at https://tgftp.nws.noaa.gov/data/rfc/lmrfc/misc/")
    #A Required Function
    def extract_number_from_href(href):
        start_index = href.find(partial_query) + len(partial_query)  # Find the index right after the string
        number = int(href[start_index+1:start_index+1+14])  # Extract the number as an integer
        return number
    
    #Calls LMRFC tgftp Site
    page_url = "https://tgftp.nws.noaa.gov/data/rfc/lmrfc/misc/"
    page_response = requests.get(page_url)
    page_soup = BeautifulSoup(page_response.content, "html.parser")
    
    # Grabs the Correct NAEFS Product
    partial_query = "HECRAS_NAEFS_pixml_export"
    query = f"a[href*=\'{partial_query}\']" 
    wrong_elements = page_soup.select(query)
    elements = [element for element in wrong_elements if element.get('href').endswith(".gz")]
    
    #Creates a Variable with the Most Recent Version
    if elements:
        element_with_largest_number = max(elements, key=lambda x: extract_number_from_href(x['href']))
        logger.info(f"SUCCESS Found NAEFS Forecast as File Name --> {element_with_largest_number}")
    else:
        utc_datetime = datetime.datetime.utcnow()
        formatted_datetime = utc_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')
        logger.info(f"ERROR: No NAEFS Forecast Found! Script Aborted at: {formatted_datetime}")
        sys.exit(1)
    
    #Saves the Most Recent NAEFS Forecast PI XML File 
    file_url = "https://tgftp.nws.noaa.gov/data/rfc/lmrfc/misc/"+element_with_largest_number["href"]
    logger.info(f"Grabbing NAEFS Forecast at --> {file_url}")

    #Un-Zip the File
    response = requests.get(file_url)
    response.raise_for_status()
    compressed_file = io.BytesIO(response.content)
    with gzip.GzipFile(fileobj=compressed_file, mode='rb') as f:
        xml_data = f.read().decode('utf-8')
    
    # Create an in-memory BytesIO object
    xml_file = io.BytesIO(xml_data.encode('utf-8'))
    logger.info(f"SUCCESS PI XML File Ready")
    return xml_file

#Parses Supplied PI XML File, Saves Data Based on Supplied Time Series and Location Groups, and Writes to CDA

#Gets Time Series and Location Group Alias Information and Combines to a Single Data Frame
def get_CWMS_TS_Loc_Data(office):
    # Create a DF with the Time series in the "Default" TS Group
    df = cwms.get_timeseries_group(group_id="Default",category_id="Default",office_id="CWMS").df

    # Format the DF
    df[["location-id", "param", "type", "int", "dur", "ver"]] = df["timeseries-id"].str.split(".", expand=True)
    df = df[df["office-id"] == office]
    df["base-loc"] = df["location-id"].str.split("-", expand=True)[0]
    if "alias-id" not in df.columns:
        df["alias-id"] = np.nan
    if "attribute" not in df.columns:
        df["attribute"] = np.nan
    df = df.rename(columns={"alias-id": "NWS_Method_TS"})
    
    # Grab the Location Group Information
    #NWS Handbook 5 Locations
    Locdf = cwms.get_location_group(loc_group_id="NWS Handbook 5 ID",category_id="Agency Aliases",office_id="CWMS").df.set_index('location-id')
    Locdf = Locdf[Locdf["office-id"] == office]

    NWS_alias_1 = Locdf[Locdf["alias-id"].notnull()]
    NWS_alias_1 = NWS_alias_1.rename(
        columns={"alias-id": "NWS_St_Num", "attribute": "Loc_attribute"}
    )

    #LMRFC NAEFS Local Flow Locations
    Locdf = cwms.get_location_group(loc_group_id="NAEFS LCL",category_id="LMRFC",office_id=office).df.set_index('location-id')
    Locdf = Locdf[Locdf["office-id"] == office]
    NWS_alias_2 = Locdf[Locdf["alias-id"].notnull()]
    NWS_alias_2 = NWS_alias_2.rename(
        columns={"alias-id": "NWS_St_Num", "attribute": "Loc_attribute"}
    )

    #Merge the Two NWS Locations Together
    NWS_alias = pd.concat([NWS_alias_1,NWS_alias_2], axis=0)

    # Merge the TS to the NWS Locations
    NWS_ts = pd.merge(df, NWS_alias, how="left",
                       on=["location-id", "office-id"])
    NWS_ts_base = pd.merge(
        NWS_ts[NWS_ts.NWS_St_Num.isnull()].drop(
            ["NWS_St_Num", "Loc_attribute"], axis=1
        ),
        NWS_alias,
        left_on=["base-loc", "office-id"],
        right_on=["location-id", "office-id"],
    )
    NWS_ts = pd.concat(
        [NWS_ts[NWS_ts["NWS_St_Num"].notnull()], NWS_ts_base], axis=0
    )

    #Simplify for Later Use
    df = NWS_ts
    LocTS_df = df[['NWS_St_Num','location-id','param','timeseries-id']]
    return LocTS_df

#Grabs Parsed Data from the PI XML File, Converts it to JSON, and Saves to CDA
def load_NAEFS_data(row,LocTS_df,NS,creationDate,creationTime,root):
    # Grab some Variables
    site_location = row[1].upper()
    USACE_site = row[2]
    site_parameter = row[3].upper()
    site_timeseries = row[4]
    logger.info(f"Checking for Data at --> {site_location}")

    #Find the NWS Station and appropriate associated parameter
    for series in root.findall('pi:series',NS):
        #Parse Headers
        header = series.find('pi:header',NS)
        NWS_Location = header.find('pi:locationId',NS).text
        Parameter = header.find('pi:parameterId',NS).text
        Units = header.find('pi:units',NS).text
        NWS_Missing = header.find('pi:missVal',NS).text

        if NWS_Location == site_location and Parameter == site_parameter:
            logger.info(f"SUCCESS Found Data for --> {site_location}")
            x = 1
            #Get Creation Date for Version Time Series
            forecast_datetime = creationDate+" "+creationTime
            forecast_datetime = datetime.datetime.strptime(forecast_datetime,'%Y-%m-%d %H:%M:%S')
            forecast_datetime = forecast_datetime.replace(hour=12,minute=0,second=0)
            
            #Format the Forecast Date to Use as the Version Date
            forecast_tz = pytz.timezone('UTC')
            forecast_datetime = forecast_datetime.astimezone(forecast_tz)
            forecast_datetime = forecast_datetime.strftime('%Y-%m-%dT%H:%M:%S%z')

            #Grab the Actual Data
            logger.info(f"Saving data as --> {USACE_site} to TSID --> {site_timeseries}")
            dateTimes = []
            values = []
            qualities = []
            for event in series.findall('pi:event',NS):
                date = event.get('date')
                time = event.get('time')
                Datetime = f"{date} {time}"
                value = event.get('value')
                quality = 0
                if value == NWS_Missing:
                    value = Missing_Value
                    quality = Missing_Quality_Code
                dateTimes.append(Datetime)
                values.append(value)
                qualities.append(quality)
            
            #Convert to a Pandas Data Frame and Add the Quality Code
            data_df = pd.DataFrame({'date-time':dateTimes,'value':values,'quality-code':qualities})

            #Format the Datetime Column
            data_df['date-time'] = pd.to_datetime(data_df["date-time"])
            data_df['date-time'] = data_df['date-time'].dt.tz_localize('UTC')
            data_df['date-time'] = data_df['date-time'].dt.strftime("%Y-%m-%d %H:%M:%S%z")

            #Write Data to CDA
            logger.info(f"Writing Data to CDA for --> {site_location}")
            try:
                data_json = cwms.timeseries_df_to_json(data=data_df,ts_id=site_timeseries,units=Units,office_id=OFFICE,version_date=forecast_datetime)
                cwms.store_timeseries(data_json)
                logger.info(
                    f"SUCCESS Forecast Stored in CWMS Database for --> {site_location},{site_timeseries}"
                )
            except Exception as error:
                storErr.append([site_timeseries,error])
                logger.error(
                    f"FAIL Forecast Could not be Stored in CWMS Database for --> {site_location},{site_timeseries}. CDA Error --> {error}"
                )
            break #Ends the search for row, the NWS Site Location, in the PI XML File
        else:
            x = 0
    if x == 0:
        logger.info(f"WARNING No Data Found for --> {site_location}")

#Runs the Main Script Call
def main():
    #Some Admin/TimeStamp Stuff
    utc_datetime = datetime.datetime.utcnow()
    formatted_datetime = utc_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')
    logger.info(f"Script Call was Initialed at: {formatted_datetime}")
    logger.info(f"CDA Connection: {APIROOT}")

    # Get the PI XML File and Parse It
    xml_file = get_NAEFS_PIXML()
    
    # Grab the Location and Time Series Groups
    LocTS_df = get_CWMS_TS_Loc_Data(OFFICE)
    logger.info(f"The Following Locations will be Saved to the Specified Time Series: {LocTS_df}")
    
    # Specify Namespace and Grab XML Root
    NS = {'pi': 'http://www.wldelft.nl/fews/PI'}
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Grab a Single Creation Date & Time to Use for All Locations
    for series in root.findall('pi:series',NS):
        header = series.find('pi:header',NS)
        try:
            creationDate = header.find('pi:creationDate', NS).text
            creationTime = header.find('pi:creationTime', NS).text
            break
        except:
            logger.info(f"There are no Creation Date or Time in the Entire PI XML File")
    logger.info(f"Found the following Creation Date & Time --> {creationDate},{creationTime}")
    
    #Loop Through Locations and Get Data to Save via CDA
    for row in LocTS_df.itertuples():
        load_NAEFS_data(row,LocTS_df,NS,creationDate,creationTime,root)
    
    #Log When the Script Ended
    utc_datetime = datetime.datetime.utcnow()
    formatted_datetime = utc_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')
    logger.info(f"Script Call Ended at: {formatted_datetime}")

## Runs the Command
if __name__ == "__main__":
    main()