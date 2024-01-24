import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from CWMS import CWMS
import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import get_current_context


APIROOT = "https://cwms-data-test.cwbi.us/cwms-data/"
APIKEY = "YtjeJIj35zjqshNLUClQNrl0nBKMNCO6jrLqBL1kILG805Tcf8JPJghz41pfrrHyffrT7wFWX0SfX5TeQOgXwC2V1uUdtV8aPHlpQVGJDfI1PoDqeaRW2o9g6i3jRnMVJIKV2FceGWM4UZM92D0xh6zKDIYw6CPaXuBZwXea35vSXj9qQYS7LNHqlUT0rmeI7wy8xZ4Ihd1dhxJIdD3UsnwxLwCokcWCxpSRCrkn93Grp7xUrNi5QkRAbsZoL5ZX"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": (datetime.utcnow() - timedelta(hours=4)).replace(minute=0, second=0),
    "catchup_by_default": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


def get_USGS_params():
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


def get_CMWS_TS_Loc_Data(cwms, office):
    """
    get time series group and location alias information and combine into singe dataframe

    """
    df = cwms.retreive_ts_group(
        "USGS TS Data Acquisition",
        "Data Acquisition",
        "CWMS",
        return_type="df",
    )
    if "attribute" not in df.columns:
        df["attribute"] = np.nan

    df[["location-id", "param", "type", "int", "dur", "ver"]] = df[
        "timeseries-id"
    ].str.split(".", expand=True)
    df = df[df["office-id"] == office]

    Locdf = cwms.retreive_loc_group(
        "USGS Station Number", "Agency Aliases", "CWMS", return_type="df"
    ).set_index("location-id")
    Locdf = Locdf[Locdf["office-id"] == office]
    # Grab all of the locations that have a USGS station number assigned to them
    USGS_alias = Locdf[Locdf["alias-id"].notnull()]
    # rename the columns
    USGS_alias = USGS_alias.rename(
        columns={"alias-id": "USGS_St_Num", "attribute": "Loc_attribute"}
    )
    # pad the USGS id with 0s if they are not 8 digits long
    USGS_alias.USGS_St_Num = USGS_alias.USGS_St_Num.str.rjust(8, "0")
    # do an inner join with the time series that are in the USGS time series group and the location group.  Join based on the Location ID
    USGS_ts = df.join(
        USGS_alias,
        on="location-id",
        lsuffix="ts",
        rsuffix="loc",
        how="inner",
    )
    USGS_Params = get_USGS_params()
    # this code fills in the USGS_Params field with values in the Time Series Group Attribute if it exists.  If it does not exist it
    # grabs the default USGS paramter for the coresponding CWMS parameter
    USGS_ts.attribute = USGS_ts.apply(
        lambda x: np.where(
            x.attribute > 0,
            str(x.attribute).split(".")[0],
            USGS_Params.at[x.param, "USGS_PARAMETER"],
        ),
        axis=1,
    ).astype("string")
    USGS_ts.attribute = USGS_ts.attribute.str.rjust(5, "0")
    # renames the attribute column to USGS_PARAMETER
    USGS_ts = USGS_ts.rename(columns={"attribute": "USGS_PARAMETER"})
    logging.info(f"CWMS TS Groups and Location Data Obtained")
    return USGS_ts


def getUSGS_ts(sites, startDT, endDT):
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
        # "parameterCd": ",".join(unique_param_codes),
        #'period': 'P1D',
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
    logging.info(f"Data obtained from USGS")
    return USGS_data


def CWMS_writeData(cwms, USGS_ts, USGS_data):
    # lists to hold time series that fail
    # noData -> usgs location and parameter were present in USGS api but the values were empty
    # NotinAPI -> usgs location and parameter were not retrieved from USGS api
    # storErr -> an error occured when saving data to CWMS database
    noData = []
    NotinAPI = []
    storErr = []

    # loop through all rows in the USGS_ts dataframe
    for index, row in USGS_ts.iterrows():
        # grab the CWMS time series if and the USGS station numbuer plus USGS parameter code
        tsId = row["timeseries-id"]
        USGS_Id_param = f"{row.USGS_St_Num}.{row.USGS_PARAMETER}"
        # check if the USGS st number and para code are in the data obtain from USGS api
        logging.info(f"Attempting to write values for tsid -->  {tsId},{USGS_Id_param}")
        if USGS_Id_param in USGS_data.index:
            # grab the time series values obtained from USGS API.
            values = pd.DataFrame(USGS_data.loc[USGS_Id_param]["values"][0]["value"])
            # if values array is empty then append infor to noData list
            if values.empty:
                noData.append([tsId, USGS_Id_param])
                logging.warning(
                    f"FAIL No Data obtained from USGS for tsid: Values array is empty in USGS API output-->  {tsId},{USGS_Id_param}"
                )
            else:
                # grab value  and for no data (ie -999999) remove from dataset
                nodata_val = USGS_data.loc[USGS_Id_param]["variable"]["noDataValue"]
                values = values[values.value != str(int(nodata_val))]
                # check again if values dataframe is empty after removing nodata_vals
                if values.empty:
                    noData.append([tsId, USGS_Id_param])
                    logging.warning(
                        f"FAIL No Data obtained from USGS for tsid: Values array is empty after removing -999999 values-->  {tsId},{USGS_Id_param}"
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
                    values.tsId = tsId  # + '-CDA'#tsId.replace('Web','CDA')

                    values.units = USGS_data.loc[USGS_Id_param]["variable"]["unit"][
                        "unitCode"
                    ]
                    values.office = row["office-idts"]
                    values["quality-code"] = 0

                    # write values to CWMS database
                    x = cwms.write_ts(values)

                    # check if data stored scucessfully
                    if x.content != b"":
                        message = json.loads(x.content.decode("UTF-8"))
                        if message["message"] == "Internal Error":
                            storErr.append([tsId, USGS_Id_param, message])
                            logging.error(
                                f"FAIL Data could not be stored to CWMS database for -->  {tsId},{USGS_Id_param} CDA error = {message}"
                            )
                    else:
                        logging.info(
                            f"SUCCESS Data stored in CWMS database for -->  {tsId},{USGS_Id_param}"
                        )
        else:
            NotinAPI.append([tsId, USGS_Id_param])
            logging.warning(
                f"FAIL USGS ID and parameter were not present in USGS API for-->  {tsId},{USGS_Id_param}"
            )


@dag(
    default_args=default_args,
    tags=["CWMS", "USGS"],
    schedule="@hourly",
    max_active_runs=1,
    max_active_tasks=4,
    catchup=False,
    doc_md=__doc__,
)
def CWMS_USGS_timeseries():
    offices = ["LRL"]
    for office in offices:
        with TaskGroup(group_id=f"{office}_USGS") as tg:
            apiKey = "apikey " + APIKEY
            cwms = CWMS()
            cwms.connect(APIROOT, apiKey=apiKey)

            @task(task_id=f"{office}_CWMS_USGS_TS_byOffice")
            def CWMS_USGS_TS_byOffice(office):
                context = get_current_context()
                ti = context["ti"]
                execution_date = ti.execution_date

                USGS_ts = get_CMWS_TS_Loc_Data(cwms, office)

                # grab all of the unique USGS stations numbers to be sent to USGS api
                sites = USGS_ts.USGS_St_Num.unique()
                logging.info(f"Execution date {execution_date}")

                # This is added to the 'startDT'
                tw_delta = -timedelta(hours=24)

                # Set the execution date and time window for URL
                startDT = execution_date + tw_delta

                # Airflow only looks at the last period during an execution run,
                # so to ensure the latest data is retrieved, add 2 hours to end date
                endDT = execution_date + timedelta(hours=2)

                logging.info(f"Grabing data from USGS between {startDT} and {endDT}")
                USGS_data = getUSGS_ts(sites, startDT, endDT)

                CWMS_writeData(cwms, USGS_ts, USGS_data)

            CWMS_USGS_TS_byOffice(office)


DAG_ = CWMS_USGS_timeseries()
