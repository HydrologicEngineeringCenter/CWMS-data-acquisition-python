#!/bin/env python3
# This getUSGS script works with CDA version 20250305.
# and cwms-python version 0.6

import logging as lg
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import cwms
import requests
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter


# create logging for logging
logging = lg.getLogger()
if (logging.hasHandlers()):
    logging.handlers.clear()
handler = lg.StreamHandler()
formatter = lg.Formatter(
    "%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
logging.addHandler(handler)
logging.setLevel(lg.INFO)
logging.propagate = False


def getusgs_cda(api_root, office_id, days_back, api_key):
    api_key = "apikey " + api_key
    cwms.api.init_session(api_root=api_root, api_key=api_key)
    logging.info(f"CDA connection: {api_root}")
    logging.info(f"Data will be grabbed and stored from USGS for past {days_back} days")
    execution_date = datetime.now()

    USGS_ts = get_CMWS_TS_Loc_Data(office_id)

    # grab all of the unique USGS stations numbers to be sent to USGS api
    sites = USGS_ts[USGS_ts["USGS_Method_TS"].isna()].USGS_St_Num.unique()
    method_sites = USGS_ts[USGS_ts["USGS_Method_TS"].notna()].USGS_St_Num.unique()
    logging.info(f"Execution date {execution_date}")

    # This is added to the 'startDT'
    tw_delta = -timedelta(days_back)

    # Set the execution date and time window for URL
    startDT = execution_date + tw_delta

    # Airflow only looks at the last period during an execution run,
    # so to ensure the latest data is retrieved, add 2 hours to end date
    endDT = execution_date + timedelta(hours=2)

    logging.info(f"Grabing data from USGS between {startDT} and {endDT}")

    USGS_data = pd.DataFrame()
    USGS_data_method = pd.DataFrame()

    if len(sites) > 0:
        USGS_data = getUSGS_ts(sites, startDT, endDT)
    # sites with a method_id or usgs tsid are retrieved from a seperate database. this is access using 3 as access in
    # usgs API call.
    if len(method_sites) > 0:
        USGS_data_method = getUSGS_ts(method_sites, startDT, endDT, 3)

    CWMS_writeData(USGS_ts, USGS_data, USGS_data_method)


def get_USGS_params():
    # defines USGS standard parameters.
    columns = [
        "USGS_PARAMETER",
        "USGS_Alias",
        "CWMS_PARAMETER",
        "CWMS_FACTOR",
        "CWMS_UNIT",
        "CWMS_TYPE",
    ]
    data = [
        ["00010", "Water Temp", "Temp-Water", 1, "C", "Inst"],
        ["00021", "Air Temp", "Temp-Air", 1, "F", "Inst"],
        ["00035", "Wind Speed", "Speed-Wind", 1, "mph", "Inst"],
        ["00036", "Wind Dir", "Dir-Wind", 1, "deg", "Inst"],
        ["00045", "Precip", "Precip-Inc", 1, "in", "Total"],
        ["00045", "Precip", "Precip", 1, "in", "Total"],
        ["00052", "RelHumidity", "%-Humidity", 1, "%", "Inst"],
        ["00060", "Flow", "Flow", 1, "cfs", "Inst"],
        # ['00061','Flow',1,'cfs','Inst'],
        ["00065", "Stage", "Stage", 1, "ft", "Inst"],
        ["00095", "Sp Cond", "Cond", 1, "umho/cm", "Inst"],
        ["00096", "Salinity", "Conc-Salinity", 0.001, "mg/l", "Inst"],
        # ['00062','Elevation','Elev',1,'ft','Inst'],
        ["72036", "Res Storage", "Stor", 1000, "ac-ft", "Inst"],
        ["62608", "Sol Rad", "Irrad-Solar", 1, "W/m2", "Inst"],
        # ['62614','Elev-Lake','Elev',1,'ft','Inst'],
        ["63160", "Elev-NAVD88", "Elev", 1, "ft", "Inst"],
    ]
    USGS_Params = pd.DataFrame(data, columns=columns).set_index("CWMS_PARAMETER")
    return USGS_Params


def get_CMWS_TS_Loc_Data(office):
    """
    get time series group and location alias information and combine into singe dataframe

    """

    def find_usgsparam(attribute, param):
        if attribute > 0:
            usgs_param = str(attribute).split(".")[0]
        elif param in USGS_Params.index:
            usgs_param = USGS_Params.at[param, "USGS_PARAMETER"]
        else:
            usgs_param = "Not Found"
        return usgs_param

    df = cwms.get_timeseries_group(
        group_id="USGS TS Data Acquisition",
        category_id="Data Acquisition",
        office_id=office,
        category_office_id="CWMS",
        group_office_id="CWMS",
    ).df

    df[["location-id", "param", "type", "int", "dur", "ver"]] = df[
        "timeseries-id"
    ].str.split(".", expand=True)

    df = df[df["office-id"] == office]
    df["base-loc"] = df["location-id"].str.split("-", expand=True)[0]
    if "alias-id" not in df.columns:
        df["alias-id"] = np.nan
    if "attribute" not in df.columns:
        df["attribute"] = np.nan
    df = df.rename(columns={"alias-id": "USGS_Method_TS"})

    # error in CDA with category_office_id and group_office_id. need to fix once CDA is updated
    Locdf = cwms.get_location_group(
        loc_group_id="USGS Station Number",
        category_id="Agency Aliases",
        office_id="CWMS",
    ).df.set_index("location-id")

    Locdf = Locdf[Locdf["office-id"] == office]
    if "attribute" not in Locdf.columns:
        Locdf["attribute"] = np.nan
    # Grab all of the locations that have a USGS station number assigned to them
    USGS_alias = Locdf[Locdf["alias-id"].notnull()]
    # rename the columns
    USGS_alias = USGS_alias.rename(
        columns={"alias-id": "USGS_St_Num", "attribute": "Loc_attribute"}
    )
    # pad the USGS id with 0s if they are not 8 digits long
    USGS_alias.USGS_St_Num = USGS_alias.USGS_St_Num.str.rjust(8, "0")

    # do an inner join with the time series that are in the USGS time series group and the location group.  Join based on the Location ID and office if
    USGS_ts = pd.merge(df, USGS_alias, how="left", on=["location-id", "office-id"])
    # grab time series with missing USGS_St_Num and check to see if the base location has an assigned USGS station.
    if USGS_ts.USGS_St_Num.isnull().any():
        USGS_ts_base = pd.merge(
            USGS_ts[USGS_ts.USGS_St_Num.isnull()].drop(
                ["USGS_St_Num", "Loc_attribute"], axis=1
            ),
            USGS_alias,
            left_on=["base-loc", "office-id"],
            right_on=["location-id", "office-id"],
        )
        # merge with existing dataframe
        USGS_ts = pd.concat(
            [USGS_ts[USGS_ts["USGS_St_Num"].notnull()], USGS_ts_base], axis=0
        )

    USGS_Params = get_USGS_params()
    # this code fills in the USGS_Params field with values in the Time Series Group Attribute if it exists.  If it does not exist it
    # grabs the default USGS paramter for the coresponding CWMS parameter
    USGS_ts.attribute = USGS_ts.apply(
        lambda x: find_usgsparam(x.attribute, x.param), axis=1
    ).astype("string")
    USGS_ts.attribute = USGS_ts.attribute.str.rjust(5, "0")
    # renames the attribute column to USGS_PARAMETER
    USGS_ts = USGS_ts.rename(columns={"attribute": "USGS_PARAMETER"})

    logging.info("CWMS TS Groups and Location Data Obtained")
    return USGS_ts


def getUSGS_ts(sites, startDT, endDT, access=None):
    """
    Function to grab data from the USGS based off of dataretieve-python
    """

    # Get USGS data
    base_url = "https://waterservices.usgs.gov/nwis/iv/?"

    query_dict = {
        "format": "json",
        "sites": ",".join(sites),
        "startDT": startDT.isoformat(),
        "endDT": endDT.isoformat(),
        "access": access,
        # "parameterCd": ",".join(unique_param_codes),
        # 'period': 'P1D',
        # "modifiedSince": "PT6H",
        "siteStatus": "active",
    }

    r = requests.get(base_url, params=query_dict).json()

    # format the responce from USGS API into dataframe
    USGS_data = pd.DataFrame(r["value"]["timeSeries"])
    USGS_data["Id.param"] = (
        USGS_data.name.str.split(":").str[1]
        + "."
        + USGS_data.name.str.split(":").str[2]
    )
    USGS_data = USGS_data.set_index("Id.param")

    logging.info("Data obtained from USGS")
    return USGS_data


def CWMS_writeData(USGS_ts, USGS_data, USGS_data_method):
    # lists to hold time series that fail
    # noData -> usgs location and parameter were present in USGS api but the values were empty
    # NotinAPI -> usgs location and parameter were not retrieved from USGS api
    # storErr -> an error occured when saving data to CWMS database
    noData = []
    NotinAPI = []
    storErr = []
    mult_ids = []
    total_recs = len(USGS_ts.index)
    saved = 0

    # loop through all rows in the USGS_ts dataframe
    for index, row in USGS_ts.iterrows():
        # grab the CWMS time series if and the USGS station numbuer plus USGS parameter code
        ts_id = row["timeseries-id"]
        USGS_Id_param = f"{row.USGS_St_Num}.{row.USGS_PARAMETER}"
        # check if the USGS st number and para code are in the data obtain from USGS api
        logging.info(
            f"Attempting to write values for ts_id -->  {ts_id},{USGS_Id_param}"
        )
        values = pd.DataFrame()
        USGS_data_row = None
        if (USGS_Id_param in USGS_data.index) and pd.isna(row.USGS_Method_TS):
            USGS_data_row = USGS_data.loc[USGS_Id_param]
        elif (USGS_Id_param in USGS_data_method.index):
            USGS_data_row = USGS_data_method.loc[USGS_Id_param]
        if USGS_data_row is not None:
            try:

            # grab the time series values obtained from USGS API.
                values_df = pd.DataFrame(USGS_data_row["values"])
                if values_df.shape[0] > 1:
                    if pd.isna(row.USGS_Method_TS):
                        logging.warning(
                            f"FAIL there are multiple time series for {USGS_Id_param} need to specify the USGS method TSID for {ts_id}"
                        )
                        mult_ids.append([ts_id, USGS_Id_param])
                    else:
                        temp = values_df.method.apply(pd.Series)
                        temp = values_df.join(pd.json_normalize(temp.pop(0)))
                        try:
                            values = pd.DataFrame(
                                temp.query(f"methodID == {row.USGS_Method_TS}")[
                                    "value"
                                ].item()
                            )
                        except Exception as error:
                            mult_ids.append([ts_id, USGS_Id_param])
                            logging.error(
                                f"The USGS method ID defined could not be found from the USGS API check that it is correct for -->  {ts_id},{USGS_Id_param},{row.USGS_Method_TS}"
                            )
                else:
                    values = pd.DataFrame(values_df.loc[0, "value"])
                # if values array is empty then append infor to noData list
                if values.empty:
                    noData.append([ts_id, USGS_Id_param])
                    logging.warning(
                        f"FAIL No Data obtained from USGS for ts_id: Values array is empty in USGS API output-->  {ts_id},{USGS_Id_param}"
                    )
                else:
                    # grab value  and for no data (ie -999999) remove from dataset
                    nodata_val = USGS_data_row["variable"]["noDataValue"]
                    values = values[values.value != str(int(nodata_val))]
                    # check again if values dataframe is empty after removing nodata_vals
                    if values.empty:
                        noData.append([ts_id, USGS_Id_param])
                        logging.warning(
                            f"FAIL No Data obtained from USGS for ts_id: Values array is empty after removing -999999 values-->  {ts_id},{USGS_Id_param}"
                        )
                    # if values are present grab information needed to save to CWMS database using CDA
                    else:
                        values = values.reindex(columns=["dateTime", "value", "qualifiers"])
    
                        # adjust column names to fit cwms-python format.
                        values = values.rename(
                            columns={
                                "dateTime": "date-time",
                                "qualifiers": "quality-code",
                            }
                        )
                        units = USGS_data_row["variable"]["unit"]["unitCode"]
                        office = row["office-id"]
                        values["quality-code"] = 0
    
                        # write values to CWMS database
                        try:
                            data = cwms.timeseries_df_to_json(
                                data=values, ts_id=ts_id, units=units, office_id=office
                            )
                            cwms.store_timeseries(data)
                            logging.info(
                                f"SUCCESS Data stored in CWMS database for -->  {ts_id},{USGS_Id_param}"
                            )
                            saved = saved + 1
                        except Exception as error:
                            storErr.append([ts_id, USGS_Id_param, error])
                            logging.error(
                                f"FAIL Data could not be stored to CWMS database for -->  {ts_id},{USGS_Id_param} CDA error = {error}"
                            )
            except Exception as error:
                logging.error(
                                f"FAIL Unspecified Error when trying to save USGS data -->  {ts_id},{USGS_Id_param} error = {error}"
                            )  
        else:
            NotinAPI.append([ts_id, USGS_Id_param])
            logging.warning(
                f"FAIL USGS ID and parameter were not present in USGS API for-->  {ts_id},{USGS_Id_param}"
            )

    logging.info(
        f"A total of {saved} records were successfully saved out of {total_recs}"
    )
    logging.info(
        f"The following ts_ids errored due to no data received from USGS for the time period requested: {noData}"
    )
    logging.info(
        f"The following ts_ids errored because the USGS ID and parameter were not found in USGS API {NotinAPI}"
    )
    logging.info(f"The following ts_ids errored when storing into CDA {storErr}")
    logging.info(
        f"The following ts_ids errored because multiple method TSID were present for the USGS station. A USGS method TSID needs to be defined in the time series group in CWMS or an incorrect TSID is defined. {mult_ids}"
    )

def main() -> None :
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--days_back", default="1", help="Days back from current time to get data.  Can be decimal and integer values")
    parser.add_argument("-o", "--office", required=True, type=str, help="Office to grab data for (Required).")
    parser.add_argument("-a", "--api_root", required=True, type=str, help="Api Root for CDA (Required).")
    parser.add_argument("-k", "--api_key", default=None, type=str, help="api key. one of api_key or api_key_loc are required")
    parser.add_argument("-kl", "--api_key_loc", default=None, type=str, help="file storing Api Key. One of api_key or api_key_loc are required")
    args = vars(parser.parse_args())

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
    getusgs_cda(
                    api_root=APIROOT,
                    office_id=OFFICE,
                    days_back=DAYS_BACK,
                    api_key=APIKEY,
                )

if __name__ == "__main__":
    main()

