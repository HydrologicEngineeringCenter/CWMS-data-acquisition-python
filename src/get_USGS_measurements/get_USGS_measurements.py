import pandas as pd
from datetime import datetime, timedelta, timezone
import pytz
import math
import numpy as np
import cwms
import logging as lg
from dataretrieval import nwis
from dotenv import load_dotenv
import os
import requests
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from collections import defaultdict

# --- Constants ---
CWMS_MISSING_VALUE = -340282346638528859811704183484516925440

TZ_MAPPING = {
    "AST": "America/Puerto_Rico",
    "EST": "America/New_York",
    "EDT": "America/New_York",
    "CST": "America/Chicago",
    "CDT": "America/Chicago",
    "MST": "America/Denver",
    "MDT": "America/Denver",
    "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
    "AKST": "America/Anchorage",
    "AKDT": "America/Anchorage",
    "HST": "Pacific/Honolulu",
    "GST": "Pacific/Guam",
}

COLUMN_MAPPING = {
    "agency_cd": "usgs_agency_cd",
    "site_no": "usgs_site_no",
    "measurement_nu": "number",
    "measurement_dt": "usgs_measurement_dt",
    "tz_cd": "usgs_tz_cd",
    "q_meas_used_fg": "used",
    "party_nm": "party",
    "site_visit_coll_agency_cd": "agency",
    "discharge_va": "flow",
    "gage_height_va": "gage-height",
    "gage_va_change": "delta-height",
    "gage_va_time": "delta-time",
    "measured_rating_diff": "quality",
    "control_type_cd": "control-condition",
    "discharge_cd": "flow-adjustment",
    "chan_nu": None,
    "chan_name": None,
    "meas_type": None,
    "streamflow_method": None,
    "velocity_method": None,
    "chan_discharge": "channel-flow",
    "chan_width": "top-width",
    "chan_velocity": "avg-velocity",
    "chan_area": "effective-flow-area",
    "chan_stability": None,
    "chan_material": None,
    "chan_evenness": None,
    "long_vel_desc": None,
    "horz_vel_desc": None,
    "vert_vel_desc": None,
    "chan_loc_cd": None,
    "chan_loc_dist": None,
    "location-id": "name",
    "utc_time": "instant",
}

# --- Logging Setup ---
logger = lg.getLogger(__name__)
if logger.hasHandlers():
    logger.handlers.clear()
handler = lg.StreamHandler()
formatter = lg.Formatter("%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(lg.INFO)
logger.propagate = False

# --- Functions ---


def convert_to_utc(df):
    """
    Converts a pandas DataFrame with timezone-aware datetimes to UTC using a timezone mapping.

    Args:
        df: pandas DataFrame with columns 'measurement_dt' (datetime-like) and 'tz_cd' (timezone code).

    Returns:
        pandas DataFrame with an added 'utc_time' column in UTC. Returns the original dataframe if there is an issue.
    """
    df_copy = df.copy()

    if "measurement_dt" not in df_copy.columns or "tz_cd" not in df_copy.columns:
        logger.error(
            "Error: 'measurement_dt' or 'tz_cd' column not found in DataFrame for UTC conversion."
        )
        return df_copy

    try:
        df_copy["measurement_dt"] = pd.to_datetime(
            df_copy["measurement_dt"], errors="coerce", format="ISO8601"
        )
    except Exception as e:
        logger.error(f"Error converting 'measurement_dt' to datetime: {e}")
        return df_copy

    def to_utc_single_row(row):
        dt = row["measurement_dt"]
        tz_str = row["tz_cd"]

        if pd.isna(dt):
            return pd.NaT

        if pd.isna(tz_str):
            if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                return pytz.timezone("UTC").localize(dt).astimezone(pytz.utc)
            else:
                return dt.astimezone(pytz.utc)

        try:
            iana_tz_name = TZ_MAPPING.get(tz_str)
            if iana_tz_name is None:
                logger.warning(
                    f"Unknown timezone code: '{tz_str}'. Check TZ_MAPPING. Returning NaT for this row."
                )
                return pd.NaT

            tz = pytz.timezone(iana_tz_name)
            if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                dt_aware = tz.localize(dt)
            else:
                dt_aware = dt.astimezone(tz)

            dt_utc = dt_aware.astimezone(pytz.utc)
            return dt_utc
        except pytz.exceptions.UnknownTimeZoneError:
            logger.warning(
                f"Unknown IANA timezone: '{iana_tz_name}' derived from '{tz_str}'. Returning NaT for this row."
            )
            return pd.NaT
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during UTC conversion for '{tz_str}': {e}. Returning NaT for this row."
            )
            return pd.NaT

    df_copy["utc_time"] = df_copy.apply(to_utc_single_row, axis=1)
    return df_copy


def rename_and_drop_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames columns in a pandas DataFrame based on a predefined mapping.
    If a target column name is not provided, the column is dropped.
    Only columns that exist are renamed or dropped.

    Args:
        df: The input pandas DataFrame.

    Returns:
        A new pandas DataFrame with renamed and dropped columns.
    """
    df_copy = df.copy()

    columns_to_drop = [
        col
        for col, target in COLUMN_MAPPING.items()
        if target is None and col in df_copy.columns
    ]
    df_copy = df_copy.drop(columns=columns_to_drop, errors="ignore")

    columns_to_rename = {
        col: target
        for col, target in COLUMN_MAPPING.items()
        if target is not None and col in df_copy.columns
    }
    df_copy = df_copy.rename(columns=columns_to_rename, errors="ignore")

    return df_copy


def clean_data(df):
    """
    Performs several data cleaning operations on a pandas DataFrame.

    - Converts 'Yes'/'No' in 'used' to True/False (after renaming).
    - Fills NaN values in string columns with empty strings.
    - Fills NaN values in numeric columns with pandas.NA.
    - Drops rows where both 'flow' and 'gage-height' are NaN.

    Args:
        df (pd.DataFrame): The input DataFrame to clean.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    df_cleaned = df.copy()

    if "used" in df_cleaned.columns:
        df_cleaned.loc[:, "used"] = (
            df_cleaned["used"].map({"Yes": True, "No": False}).astype(pd.BooleanDtype())
        )

    string_cols = df_cleaned.select_dtypes(include="object").columns
    numeric_cols = df_cleaned.select_dtypes(include=np.number).columns

    if not string_cols.empty:
        df_cleaned[string_cols] = df_cleaned[string_cols].replace(np.nan, "")
    if not numeric_cols.empty:
        df_cleaned[numeric_cols] = df_cleaned[numeric_cols].fillna(pd.NA)

    if "flow" in df_cleaned.columns and "gage-height" in df_cleaned.columns:
        mask = df_cleaned[["flow", "gage-height"]].isna().all(axis=1)
        df_cleaned = df_cleaned[~mask].copy()
    elif "flow" in df_cleaned.columns or "gage-height" in df_cleaned.columns:
        logger.warning(
            "Only one of 'flow' or 'gage-height' columns exists. Cannot perform combined NaN drop."
        )

    return df_cleaned


def process_usgs_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Orchestrates the processing of USGS data by applying a series of transformations:
    1. Converts 'measurement_dt' to UTC and adds 'utc_time' column.
    2. Renames and drops columns according to a predefined mapping.
    3. Performs general data cleaning (boolean mapping, NaN handling, row dropping).

    Args:
        df (pd.DataFrame): The input DataFrame containing USGS data.

    Returns:
        pd.DataFrame: The fully processed and cleaned DataFrame.
    """
    df_processed = df.copy()

    df_processed = convert_to_utc(df_processed)
    df_processed = rename_and_drop_columns(df_processed)
    df_processed = clean_data(df_processed)

    return df_processed


def remove_nan_values(data):
    """
    Recursively remove keys with None, NaN, or empty string values from a dictionary.
    """
    if isinstance(data, dict):
        return {
            k: remove_nan_values(v)
            for k, v in data.items()
            if v is not None
            and not (isinstance(v, float) and math.isnan(v))
            and not (isinstance(v, str) and v == "")
        }
    elif isinstance(data, list):
        return [remove_nan_values(elem) for elem in data if elem is not None]
    return data


def check_single_row_for_duplicates(row_to_check, df_existing):
    """
    Checks a single row for duplicates based on "number" and "instant"
    against df_existing, and identifies differences if an instant-based
    duplicate is found.

    Args:
        row_to_check: A pandas Series or a DataFrame with a single row
                      representing the item to check.
        df_existing: The DataFrame to compare against. Its 'number' and 'instant'
                     columns will be temporarily converted for comparison.

    Returns:
        A tuple containing:
            - original_row_passed_in: The original pandas Series or 1-row DataFrame
                                      that was passed into the function.
            - is_rejected: A boolean (True if the row was rejected due to a
                           duplicate number or a close instant, False otherwise).
            - df_differences: A DataFrame detailing specific value differences
                              between the rejected incoming row and the closest
                              existing row. This DataFrame is populated ONLY if
                              is_rejected is True due to an instant duplicate
                              AND there are actual value differences.
                              Columns: ['Column Name', 'Incoming Value', 'Existing Value'].
                              Returns an empty DataFrame otherwise.
    """
    original_input_for_return = row_to_check

    if isinstance(row_to_check, pd.Series):
        df_store_internal = row_to_check.to_frame().T
    elif isinstance(row_to_check, pd.DataFrame) and len(row_to_check) == 1:
        df_store_internal = row_to_check
    else:
        raise ValueError(
            "row_to_check must be a pandas Series or a DataFrame with a single row."
        )

    is_rejected = False
    df_differences = pd.DataFrame(
        columns=["Column Name", "Incoming Value", "Existing Value"]
    )

    if df_existing.empty:
        return original_input_for_return, is_rejected, df_differences

    df_store_compare = df_store_internal.copy()
    df_existing_compare = df_existing.copy()
    
    # cast number columns as int, sometimes USGS won't resolve to int...drop those rows
    df_invalid = df_store_compare[pd.to_numeric(df_store_compare['number'], errors='coerce').isna()]
    if not df_invalid.empty:
        logger.info(f"Can't resolve measurement numbers {df_invalid['number'].values} to number. Won't store those measurements")

    # Convert the valid rows to numeric and drop the invalid ones
    df_store_compare['number'] = pd.to_numeric(df_store_compare['number'], errors='coerce')  # Convert to numeric, coercing errors to NaN
    df_store_compare = df_store_compare.dropna(subset=['number'])  # Drop rows where 'number' is NaN

    df_store_compare["number"] = df_store_compare["number"].astype(int)
    df_existing_compare["number"] = df_existing_compare["number"].astype(int)

    df_store_compare["instant"] = pd.to_datetime(df_store_compare["instant"])
    df_existing_compare["instant"] = pd.to_datetime(df_existing_compare["instant"])

    current_number = df_store_compare["number"].iloc[0]
    current_instant = df_store_compare["instant"].iloc[0]

    if current_number in df_existing_compare["number"].values:
        is_rejected = True
        return original_input_for_return, is_rejected, df_differences

    time_diffs = (df_existing_compare["instant"] - current_instant).abs()
    five_minutes = pd.Timedelta(minutes=5)

    if not time_diffs.empty and time_diffs.min() <= five_minutes:
        is_rejected = True

        close_matches_mask = time_diffs <= five_minutes
        close_matches = df_existing_compare[close_matches_mask]

        if not close_matches.empty:
            closest_existing_row_idx = (
                (close_matches["instant"] - current_instant).abs().idxmin()
            )
            closest_existing_row = close_matches.loc[closest_existing_row_idx]

            diff_records = []
            columns_to_compare = [
                col
                for col in df_store_compare.columns
                if col not in ["number", "instant"]
            ]

            for col in columns_to_compare:
                current_val = df_store_compare[col].iloc[0]
                existing_val = closest_existing_row.get(col)

                # Handle NaN values explicitly
                if pd.isna(current_val) and pd.isna(existing_val):
                    continue
                elif pd.isna(current_val) != pd.isna(
                    existing_val
                ):  # One is NaN, other is not
                    diff_records.append(
                        {
                            "Column Name": col,
                            "Incoming Value": current_val,
                            "Existing Value": existing_val,
                        }
                    )
                elif pd.api.types.is_numeric_dtype(
                    df_store_compare[col]
                ) and pd.api.types.is_numeric_dtype(closest_existing_row[col]):
                    # For numeric values, compare with a small tolerance
                    if (
                        abs(current_val - existing_val) > 1e-6
                    ):  # Example tolerance for floats
                        diff_records.append(
                            {
                                "Column Name": col,
                                "Incoming Value": current_val,
                                "Existing Value": existing_val,
                            }
                        )
                elif current_val != existing_val:
                    diff_records.append(
                        {
                            "Column Name": col,
                            "Incoming Value": current_val,
                            "Existing Value": existing_val,
                        }
                    )

            if diff_records:
                df_differences = pd.DataFrame(diff_records)

    return original_input_for_return, is_rejected, df_differences


def create_json_from_row(row):
    """
    Transforms a DataFrame row into the specified JSON format.
    """
    try:
        instant_value = pd.to_datetime(row["instant"]).isoformat()
    except Exception as e:
        logger.warning(
            f"Could not convert instant '{row.get('instant')}' to ISO format: {e}. Setting to None."
        )
        instant_value = None

    json_data = {
        "height-unit": "ft",
        "flow-unit": "cfs",
        "used": (
            bool(row["used"]) if pd.notna(row["used"]) else False
        ),  # Ensure proper bool conversion
        "agency": str(row["agency"]),
        "party": str(row["party"]),
        "wm-comments": f"imported from get_USGS_measurements.py {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}",
        "instant": instant_value,
        "id": {"office-id": str(row["office"]), "name": str(row["name"])},
        "number": str(row["number"]),
        "streamflow-measurement": {
            "gage-height": (
                float(row["gage-height"])
                if pd.notna(row["gage-height"])
                else CWMS_MISSING_VALUE
            ),
            "flow": (
                float(row["flow"]) if pd.notna(row["flow"]) else CWMS_MISSING_VALUE
            ),
            "quality": str(row["quality"]),
        },
        "usgs-measurement": {
            "control-condition": (
                str(row["control-condition"])
                if pd.notna(row["control-condition"])
                and row["control-condition"] != "Unspecified"  # Corrected typo
                else None
            ),
            "flow-adjustment": str(row["flow-adjustment"]),
            "delta-height": (
                float(row["delta-height"])
                if pd.notna(row["delta-height"])
                else None  # Assuming None for delta if NaN
            ),
            "delta-time": (
                float(row["delta-time"])
                if pd.notna(row["delta-time"])
                else None  # Assuming None for delta if NaN
            ),
        },
    }

    # Apply the recursive NaN remover once at the end
    json_data = remove_nan_values(json_data)
    return json_data


# --- Main Script Execution ---


parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
parser.add_argument(
    "-d",
    "--days_back_modified",
    default="2",
    help="Days back from current time measurements have been modified in USGS database. Can be integer value",
)
parser.add_argument(
    "-c",
    "--days_back_collected",
    default="365",
    help="Days back from current time measurements have been were collected. Can be integer value",
)

parser.add_argument(
    "-o",
    "--office",
    required=False,
    type=str,
    help="Office to grab data for. If not provided, will read from environment.",
)

parser.add_argument(
    "-a", "--api_root", required=False, type=str, help="Api Root for CDA. If not provided, will read from environment."
)
parser.add_argument(
    "-k",
    "--api_key",
    default=None,
    type=str,
    help="api key. If not provided, will read from environment.",
)

args = parser.parse_args()
DAYS_BACK_MODIFIED = int(args.days_back_modified)
DAYS_BACK_COLLECTED = int(args.days_back_collected)

# grab API variables from .env file
load_dotenv()


# Use command line argument if provided, otherwise fall back to environment variable
APIROOT = args.api_root if args.api_root is not None else os.getenv("API_ROOT")
APIKEY = args.api_key if args.api_key is not None else os.getenv("API_KEY")
OFFICE = args.office if args.office is not None else os.getenv("OFFICE")


# Validate environment variables
if not all([APIROOT, OFFICE, APIKEY]):
    logger.critical(
        "Missing one or more environment variables (API_ROOT, OFFICE, API_KEY). Exiting."
    )
    exit(1)

apiKey = "apikey " + APIKEY
api = cwms.api.init_session(api_root=APIROOT, api_key=apiKey)

logger.info("Fetching CWMS location groups...")
try:
    usgs_alias_group = cwms.get_location_group(
        loc_group_id="USGS Station Number",
        category_id="Agency Aliases",
        office_id="CWMS",
    )
    usgs_measurement_locs = cwms.get_location_group(
        loc_group_id="USGS Measurements",
        category_id="Data Acquisition",
        office_id="CWMS",
    )
except requests.exceptions.RequestException as e:
    logger.critical(f"Failed to fetch CWMS location groups: {e}. Exiting.")
    exit(1)
except Exception as e:
    logger.critical(
        f"An unexpected error occurred fetching CWMS location groups: {e}. Exiting."
    )
    exit(1)


# merge them together
measurement_site_df = pd.merge(
    usgs_measurement_locs.df,
    usgs_alias_group.df,
    on="location-id",
    how="inner",
    left_on=None,
    right_on=None,
)
# drop any that don't have a USGS id
measurement_site_df = measurement_site_df[measurement_site_df["alias-id"].notnull()]


if measurement_site_df.empty:
    logger.warning(
        "No valid USGS measurement locations found in CWMS after de-duplication. Exiting."
    )
    exit(0)

# Pre-create lookup for faster access in the loop
cwms_site_lookup = defaultdict(list)

for idx, row in measurement_site_df.iterrows():
    alias_id = row["alias-id"]
    cwms_site_lookup[alias_id].append(
        {
            "location-id": row["location-id"],
            "office-id_x": row["office-id_x"],
            "attribute_x": row["attribute_x"],
        }
    )

# Log if any alias-ids map to multiple CWMS configurations
for alias_id, configs in cwms_site_lookup.items():
    if len(configs) > 1:
        logger.info(
            f"USGS alias-id '{alias_id}' maps to multiple CWMS configurations: {[(c['location-id'], c['office-id_x']) for c in configs]}"
        )


execution_date = datetime.now()
startDT = execution_date - timedelta(DAYS_BACK_COLLECTED)

logger.info(
    f"Fetching USGS discharge measurements from {startDT.isoformat()} (modified in last {DAYS_BACK_MODIFIED} days)..."
)
try:

    df_meas_usgs, meta = nwis.get_discharge_measurements(
        # sites=["05058000", "05059500"],
        period=f"P{DAYS_BACK_COLLECTED}D",
        channel_rdb_info="1",
        sv_md_interval="DAY",
        sv_md=f"{DAYS_BACK_MODIFIED}",
        sv_md_minutes="2",
    )
    logger.info(f"Queried {meta}")
except Exception as e:
    logger.critical(f"Failed to fetch USGS measurements: {e}. Exiting.")
    exit(1)

if df_meas_usgs.empty:
    logger.info("No new USGS measurements found to process.")
    exit(0)

logger.info(f"Processing {len(df_meas_usgs)} USGS measurements...")
df_meas_usgs = process_usgs_data(df_meas_usgs)
total_usgs_measurements_processed = 0
total_usgs_measurements_skipped_no_cwms_mapping = 0

# This will store stats like: {'office_id_MVP': {'attempted': X, 'successful': Y, 'rejected': Z}}
office_store_stats = defaultdict(lambda: defaultdict(int))
for index, usgs_row in df_meas_usgs.iterrows():
    total_usgs_measurements_processed += 1
    site_no = usgs_row.usgs_site_no

    if site_no not in cwms_site_lookup:
        # logger.warning(
        #     f"USGS site '{site_no}' not found in CWMS lookup. Skipping measurement collected at {usgs_row.instant}."
        # )
        total_usgs_measurements_skipped_no_cwms_mapping += 1
        continue

    # Iterate over all CWMS configurations for this USGS site ---
    cwms_configs_for_site = cwms_site_lookup[site_no]
    for config_idx, cwms_config in enumerate(cwms_configs_for_site):
        cwms_loc = cwms_config["location-id"]
        office_id = cwms_config["office-id_x"]  # Get the office_id for this config
        overwrite_flag = cwms_config[
            "attribute_x"
        ]  # Assuming 1 means overwrite, 0 means don't overwrite

        # Create a copy of the row for JSON creation and modification
        usgs_row_for_json = usgs_row.copy()
        usgs_row_for_json["name"] = cwms_loc
        usgs_row_for_json["office"] = office_id

        data = create_json_from_row(usgs_row_for_json)
        office_store_stats[office_id][
            "attempted"
        ] += 1  # Increment attempted for this office

        # get existing measurements at site
        df_existing = pd.DataFrame()  # Initialize as empty
        try:
            existing_measurements = cwms.get_measurements(
                location_id_mask=cwms_loc, office_id=office_id
            )
            if existing_measurements and existing_measurements.df is not None:
                df_existing = existing_measurements.df
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while getting existing measurements for {cwms_loc} ({office_id}). Assuming no existing measurements."
            )

        _, is_rejected, df_differences = check_single_row_for_duplicates(
            usgs_row_for_json, df_existing
        )

        log_prefix = f"USGS site {site_no} -> CWMS loc {cwms_loc} ({office_id}) measurement collected at {usgs_row.instant}"

        if overwrite_flag == 1:
            try:
                logger.info(f"{log_prefix} (overwrite enabled). Storing.")
                cwms.store_measurements(data=[data], fail_if_exists=False)
                office_store_stats[office_id][
                    "successful"
                ] += 1  # Increment successful for this office
                if not df_differences.empty:
                    logger.info(
                        f"Differences found between stored data and new data for {log_prefix}:\n{df_differences.to_string()}"
                    )
            except requests.exceptions.RequestException as e:
                logger.error(f"CWMS API network error storing {log_prefix}: {e}")
                # For overwrite enabled, if it fails, it's an error, not a 'rejection' due to existing data
            except Exception as e:
                logger.error(f"Unexpected error storing {log_prefix}: {e}")
        else:  # overwrite_flag is 0 or some other value, meaning don't overwrite
            if not is_rejected:
                try:
                    logger.info(f"{log_prefix}. Storing.")
                    cwms.store_measurements(
                        data=[data]
                    )  # fail_if_exists=True by default
                    office_store_stats[office_id][
                        "successful"
                    ] += 1  # Increment successful for this office
                    if not df_differences.empty:
                        logger.info(
                            f"Differences found between stored data and new data for {log_prefix}:\n{df_differences.to_string()}"
                        )
                except requests.exceptions.RequestException as e:
                    # If fail_if_exists is True (default)
                    logger.warning(
                        f"CWMS API network error (likely duplicate or conflict) storing {log_prefix}: {e}"
                    )
                    office_store_stats[office_id][
                        "rejected"
                    ] += 1  # Increment rejected for this office
                except Exception as e:
                    logger.error(f"Unexpected error storing {log_prefix}: {e}")
            else:
                logger.warning(
                    f"{log_prefix} has same number field ({usgs_row.number}) or similar collection time as existing measurement. Not storing."
                )
                office_store_stats[office_id][
                    "rejected"
                ] += 1  # Increment rejected for this office


logger.info("-" * 50)
logger.info("Processing Summary:")
logger.info(f"Total USGS measurements fetched: {len(df_meas_usgs)}")
logger.info(
    f"Total unique USGS measurements processed for CWMS: {total_usgs_measurements_processed}"
)
logger.info(
    f"Total USGS measurements skipped (no CWMS mapping): {total_usgs_measurements_skipped_no_cwms_mapping}"
)

logger.info("\nCWMS Store Statistics Per Office:")
# Calculate global totals from office_store_stats for consistency
global_attempted = sum(stats["attempted"] for stats in office_store_stats.values())
global_successful = sum(stats["successful"] for stats in office_store_stats.values())
global_rejected = sum(stats["rejected"] for stats in office_store_stats.values())

for office, stats in sorted(office_store_stats.items()):
    logger.info(f"  Office: {office}")
    logger.info(f"    Attempted: {stats['attempted']}")
    logger.info(f"    Successful: {stats['successful']}")
    logger.info(f"    Rejected (Duplicate/Conflict): {stats['rejected']}")

logger.info("\nOverall CWMS Store Statistics:")
logger.info(
    f"Total CWMS store attempts (across all configurations): {global_attempted}"
)
logger.info(f"Total CWMS stores successful: {global_successful}")
logger.info(f"Total CWMS stores rejected (duplicate/conflict): {global_rejected}")
logger.info("-" * 50)
