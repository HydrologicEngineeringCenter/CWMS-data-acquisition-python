import hashlib
import logging as lg
import math
import os
import re
from argparse import (ArgumentDefaultsHelpFormatter, ArgumentParser,
                      BooleanOptionalAction)
from collections import defaultdict

import cwms
import numpy as np
import pandas as pd
import requests

# --- Constants ---
CWMS_MISSING_VALUE = -340282346638528859811704183484516925440

FIELD_MEASUREMENTS_URL = (
    "https://api.waterdata.usgs.gov/ogcapi/v0/collections/field-measurements/items"
)
CHANNEL_MEASUREMENTS_URL = (
    "https://api.waterdata.usgs.gov/ogcapi/v0/collections/channel-measurements/items"
)

CHANNEL_NUMERIC_COLS = [
    "channel_flow",
    "channel_width",
    "channel_area",
    "channel_velocity",
    "channel_location_distance",
]

COLUMN_MAPPING = {
    "monitoring_location_id": "usgs_site_no",
    "flow_value": "flow",
    "stage_value": "gage-height",
    "unit_of_measure": "flow-unit",
    "stage_unit_of_measure": "height-unit",
    "time": "instant",
    "measurement_rated": "quality",
    "measuring_agency": "agency",
    "control_condition": "control-condition",
    "approval_status": "used",
    "field_visit_id": "usgs_field_visit_id",
    "field_measurement_id": "usgs_field_measurement_id",  # kept for remarks
    "last_modified": "usgs_last_modified",  # kept for remarks
    # Drop these
    "observing_procedure_code": None,
    "observing_procedure": None,
    "qualifier": None,
    "vertical_datum": None,
    "geometry_type": None,
    "geometry": None,
    "location-id": "name",
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


# --- Runtime Config ---
# Initialized once by getUSGS_measurement_cda() before any fetch calls.
_usgs_session: requests.Session = requests.Session()
_fetch_channel: bool = True


# --- Functions ---


def uuid_to_measurement_number(uuid_str: str, max_digits: int = 6) -> int:
    """
    Converts a UUID string to a deterministic positive integer
    suitable for CWMS measurement_nu.

    max_digits controls the upper bound:
      6 digits -> max 999,999   (default, ~1M unique values)
      5 digits -> max 99,999
      4 digits -> max 9,999

    Note: Smaller max_digits increases collision probability.
    Collisions are handled downstream by the duplicate-check logic.
    """
    hex_digest = hashlib.md5(uuid_str.encode()).hexdigest()
    upper_bound = 10**max_digits
    return int(hex_digest, 16) % upper_bound


def fetch_field_measurements(
    parameter_code: list[str] = None,
    last_modified: str = None,
    monitoring_location_id: str | list[str] = None,
    time: str = None,
    field_visit_id: str | list[str] = None,
    limit: int = 10000,
    collection_url: str = FIELD_MEASUREMENTS_URL,
    log_progress: bool = True,
) -> pd.DataFrame:
    params = {
        "f": "json",
        "skipGeometry": "false",
        "limit": limit,
    }
    if parameter_code:
        params["parameter_code"] = ",".join(parameter_code)
    if last_modified:
        params["last_modified"] = last_modified
    if monitoring_location_id:
        params["monitoring_location_id"] = (
            ",".join(monitoring_location_id)
            if isinstance(monitoring_location_id, list)
            else monitoring_location_id
        )
    if time:
        params["time"] = time
    if field_visit_id:
        params["field_visit_id"] = (
            ",".join(field_visit_id)
            if isinstance(field_visit_id, list)
            else field_visit_id
        )

    all_features = []
    url = collection_url

    while url:
        try:
            response = _usgs_session.get(
                url,
                params=params if url == collection_url else None,
                timeout=60,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"USGS API request failed: {e}")
            raise

        data = response.json()
        features = data.get("features", [])
        all_features.extend(features)

        if log_progress:
            logger.info(
                f"Fetched {len(features)} records "
                f"(total so far: {len(all_features)})"
            )

        url = next(
            (
                link["href"]
                for link in data.get("links", [])
                if link.get("rel") == "next"
            ),
            None,
        )

    if not all_features:
        return pd.DataFrame()

    records = [f["properties"] for f in all_features]
    df = pd.DataFrame(records)

    if "id" in df.columns:
        df = df.rename(columns={"id": "field_measurement_id"})
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df


def fetch_channel_measurements(
    last_modified: str = None,
    monitoring_location_id: str | list[str] = None,
    time: str = None,
    field_visit_id: str | list[str] = None,
    limit: int = 10000,
    log_progress: bool = True,
) -> pd.DataFrame:
    df = fetch_field_measurements(
        last_modified=last_modified,
        monitoring_location_id=monitoring_location_id,
        time=time,
        field_visit_id=field_visit_id,
        limit=limit,
        collection_url=CHANNEL_MEASUREMENTS_URL,
        log_progress=log_progress,
    )

    if df.empty:
        return df

    for col in CHANNEL_NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def fetch_channel_measurements_chunked(
    visit_ids: list,
    chunk_size: int = 50,
) -> pd.DataFrame:
    if not visit_ids:
        return pd.DataFrame()

    frames = []
    total_fetched = 0
    num_chunks = math.ceil(len(visit_ids) / chunk_size)

    for i in range(0, len(visit_ids), chunk_size):
        chunk = visit_ids[i : i + chunk_size]
        chunk_num = i // chunk_size + 1
        try:
            df_chunk = fetch_channel_measurements(
                field_visit_id=chunk,
                log_progress=False,
            )
            chunk_count = len(df_chunk)
            total_fetched += chunk_count
            logger.info(
                f"Channel measurements chunk {chunk_num}/{num_chunks}: "
                f"{chunk_count} records (running total: {total_fetched})"
            )
            if not df_chunk.empty:
                frames.append(df_chunk)
        except Exception as e:
            logger.warning(
                f"Channel measurement fetch failed for chunk {chunk_num}/{num_chunks}: {e}"
            )

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_missing_pairs(
    df_raw: pd.DataFrame,
    flow_code: str = "00060",
    stage_code: str = "00065",
    chunk_size: int = 50,
) -> pd.DataFrame:
    """
    For any field_visit_id that has only a flow or only a stage record in df_raw,
    fetches the missing counterpart from the USGS API and appends it.

    This handles the case where USGS edits flow and stage records independently —
    a last_modified query may return one but not the other for the same visit.
    """
    flow_visits = set(df_raw[df_raw["parameter_code"] == flow_code]["field_visit_id"])
    stage_visits = set(df_raw[df_raw["parameter_code"] == stage_code]["field_visit_id"])
    unpaired_visits = flow_visits.symmetric_difference(stage_visits)

    if not unpaired_visits:
        logger.info("No unpaired field visits found. No supplemental fetch needed.")
        return df_raw

    logger.info(
        f"{len(unpaired_visits)} unpaired field_visit_ids found. "
        f"Fetching missing counterpart records..."
    )

    visit_list = list(unpaired_visits)
    supplemental_frames = []

    for i in range(0, len(visit_list), chunk_size):
        chunk = visit_list[i : i + chunk_size]
        try:
            df_supplement = fetch_field_measurements(
                parameter_code=[flow_code, stage_code],
                field_visit_id=chunk,
            )
            if not df_supplement.empty:
                supplemental_frames.append(df_supplement)
                logger.info(
                    f"Supplemental chunk {i // chunk_size + 1}: "
                    f"{len(df_supplement)} records returned."
                )
        except Exception as e:
            logger.warning(
                f"Supplemental fetch failed for chunk {i // chunk_size + 1}: {e}"
            )

    if not supplemental_frames:
        logger.warning("Supplemental fetch returned no records.")
        return df_raw

    df_supplement_all = pd.concat(supplemental_frames, ignore_index=True)

    # Deduplicate: only add records not already present
    existing_ids = set(df_raw["field_measurement_id"])
    df_new = df_supplement_all[
        ~df_supplement_all["field_measurement_id"].isin(existing_ids)
    ]
    logger.info(
        f"Adding {len(df_new)} supplemental records to complete unpaired visits."
    )

    return pd.concat([df_raw, df_new], ignore_index=True)


def pair_flow_stage(
    df: pd.DataFrame,
    flow_code: str = "00060",
    stage_code: str = "00065",
    match_on: str = "field_visit_id",
) -> pd.DataFrame:
    """
    Pairs flow and stage measurements from a USGS field measurement DataFrame.

    Parameters
    ----------
    df         : Source DataFrame containing both flow and stage rows.
    flow_code  : parameter_code for discharge/flow  (default '00060').
    stage_code : parameter_code for stage/gage height (default '00065').
    match_on   : Column to join on. Use 'field_visit_id' to match by visit UUID.

    Returns
    -------
    DataFrame with one row per flow measurement and an added 'stage_value' column.
    Stage-only records (no matching flow) are dropped and logged.
    """
    flow_df = df[df["parameter_code"] == flow_code].copy()
    stage_df = df[df["parameter_code"] == stage_code][
        [match_on, "value", "unit_of_measure"]
    ].copy()
    stage_df = stage_df.rename(
        columns={"value": "stage_value", "unit_of_measure": "stage_unit_of_measure"}
    )

    flow_visits = set(flow_df[match_on])
    stage_visits = set(stage_df[match_on])

    stage_only = stage_df[~stage_df[match_on].isin(flow_visits)]
    if not stage_only.empty:
        logger.info(
            f"pair_flow_stage: {len(stage_only)} stage-only records dropped "
            f"(no matching flow record for same {match_on})"
        )

    flow_only = flow_df[~flow_df[match_on].isin(stage_visits)]
    if not flow_only.empty:
        logger.info(
            f"pair_flow_stage: {len(flow_only)} flow-only records "
            f"(stage will be NaN)"
        )

    paired = flow_df.merge(stage_df, on=match_on, how="left")
    paired = paired.drop(columns=["parameter_code"], errors="ignore")
    paired = paired.reset_index(drop=True)
    paired = paired.rename(columns={"value": "flow_value"})

    cols = list(paired.columns)
    cols.remove("stage_value")
    cols.remove("stage_unit_of_measure")
    flow_idx = cols.index("flow_value")
    cols.insert(flow_idx, "stage_unit_of_measure")
    cols.insert(flow_idx, "stage_value")

    return paired[cols].reset_index(drop=True)


def rename_and_drop_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames columns in a pandas DataFrame based on COLUMN_MAPPING.
    If a target column name is None, the column is dropped.
    Only columns that exist are renamed or dropped.
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


def to_control_condition_camel(s: str) -> str:
    """
    Converts a control condition string to CamelCase.
    Handles hyphens, surrounding whitespace, and multi-word phrases.
    e.g. "Ice - Cover"          -> "IceCover"
         "Fill control changed" -> "FillControlChanged"
         "No flow"              -> "NoFlow"
         "IceCover"             -> "IceCover"  (already correct, unchanged)
    """
    s = re.sub(r"\s*-\s*", " ", s)
    return "".join(word.capitalize() for word in s.split())


def clean_data(df):
    df_cleaned = df.copy()

    if "used" in df_cleaned.columns:
        df_cleaned["used"] = (
            df_cleaned["used"]
            .map({"Approved": True, "Provisional": False, "Working": False})
            .astype(pd.BooleanDtype())
        )

    if "control-condition" in df_cleaned.columns:
        df_cleaned["control-condition"] = df_cleaned["control-condition"].apply(
            lambda x: to_control_condition_camel(str(x)) if pd.notna(x) else x
        )

    string_cols = df_cleaned.select_dtypes(include="object").columns
    numeric_cols = df_cleaned.select_dtypes(include=np.number).columns

    if not string_cols.empty:
        df_cleaned[string_cols] = df_cleaned[string_cols].astype("string").fillna("")
    if not numeric_cols.empty:
        df_cleaned[numeric_cols] = df_cleaned[numeric_cols].fillna(pd.NA)

    if "flow" in df_cleaned.columns and "gage-height" in df_cleaned.columns:
        mask = df_cleaned[["flow", "gage-height"]].isna().all(axis=1)
        dropped = mask.sum()
        if dropped > 0:
            logger.info(
                f"Dropping {dropped} rows where both flow and gage-height are NaN"
            )
        df_cleaned = df_cleaned[~mask].copy()
    elif "flow" in df_cleaned.columns or "gage-height" in df_cleaned.columns:
        logger.warning(
            "Only one of 'flow' or 'gage-height' columns exists. "
            "Cannot perform combined NaN drop."
        )

    return df_cleaned, 0


def process_usgs_data(df: pd.DataFrame) -> tuple:
    """
    Orchestrates processing of USGS waterdata API field measurement DataFrame:
    1. Pairs flow/stage rows into single rows via pair_flow_stage().
    2. Strips 'USGS-' prefix from monitoring_location_id.
    3. Generates deterministic integer measurement number from field_visit_id UUID.
    4. Renames and drops columns per COLUMN_MAPPING.
    5. Cleans data.
    """
    df_processed = df.copy()

    # Step 1: Pivot flow/stage into single rows per visit
    df_processed = pair_flow_stage(df_processed)

    # Step 2: Strip USGS- prefix for CWMS site matching
    df_processed["monitoring_location_id"] = df_processed[
        "monitoring_location_id"
    ].str.replace("USGS-", "", regex=False)

    # Step 3: Generate deterministic integer measurement number from UUID
    df_processed["number"] = df_processed["field_visit_id"].apply(
        uuid_to_measurement_number
    )

    # Step 4: Rename and drop columns
    df_processed = rename_and_drop_columns(df_processed)

    # Step 5: Clean data
    df_processed, dropped = clean_data(df_processed)

    return df_processed, dropped


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
    against df_existing, and identifies differences if a duplicate is found.

    Args:
        row_to_check: A pandas Series or a DataFrame with a single row.
        df_existing: The DataFrame to compare against.

    Returns:
        Tuple of (original_row, is_rejected, df_differences).
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

    df_store_compare["number"] = df_store_compare["number"].astype(str)
    df_existing_compare["number"] = df_existing_compare["number"].astype(str)

    df_store_compare["instant"] = pd.to_datetime(df_store_compare["instant"], utc=True)
    df_existing_compare["instant"] = pd.to_datetime(
        df_existing_compare["instant"], utc=True
    )

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

                if pd.isna(current_val) and pd.isna(existing_val):
                    continue
                elif pd.isna(current_val) != pd.isna(existing_val):
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
                    if abs(current_val - existing_val) > 1e-6:
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


def check_and_drop_duplicates(df_store, df_existing):
    """
    Checks for duplicates based on "number" and "instant" columns and drops them.

    Returns:
        Tuple of (df_store, df_rejected_number, df_rejected_instant).
    """
    if not df_existing.empty:

        df_store.loc[:, "number"] = df_store["number"].astype(str)

        df_store["instant"] = pd.to_datetime(df_store["instant"], utc=True)
        df_existing["instant"] = pd.to_datetime(df_existing["instant"], utc=True)

        mask_number = df_store["number"].isin(df_existing["number"].astype(str))
        df_rejected_number = df_store[mask_number].copy()
        df_store = df_store[~mask_number]

        df_rejected_instant = pd.DataFrame(columns=df_store.columns)
        indices_to_drop = []

        for index, row in df_store.iterrows():
            closest_time = df_existing["instant"].iloc[
                (df_existing["instant"] - row["instant"]).abs().argsort()[:1]
            ]
            if abs((closest_time.iloc[0] - row["instant"]).total_seconds()) <= 300:
                df_rejected_instant = pd.concat([df_rejected_instant, row.to_frame().T])
                indices_to_drop.append(index)

        df_store = df_store.drop(indices_to_drop)

        return df_store, df_rejected_number, df_rejected_instant
    else:
        return df_store, pd.DataFrame(), pd.DataFrame()


def create_json_from_row(row, df_channel: pd.DataFrame = None):
    try:
        instant_value = pd.to_datetime(row["instant"], utc=True).isoformat()
    except Exception as e:
        logger.warning(
            f"Could not convert instant '{row.get('instant')}' to ISO format: {e}. "
            f"Setting to None."
        )
        instant_value = None

    # Guard against empty string from clean_data's fillna("")
    field_visit_id = str(row.get("usgs_field_visit_id", "") or "unknown")
    usgs_last_modified = str(row.get("usgs_last_modified", "") or "").strip()

    remarks = f"field_visit_id={field_visit_id}"

    wm_comments = "imported from get_USGS_measurements.py"
    if usgs_last_modified:
        wm_comments += f" last_modified={usgs_last_modified}"

    # --- Channel measurement aggregation ---
    supplemental = {}
    if df_channel is not None and not df_channel.empty:
        visit_channels = df_channel[df_channel["field_visit_id"] == field_visit_id]
        if not visit_channels.empty:
            total_flow = visit_channels["channel_flow"].sum(skipna=True)
            total_width = visit_channels["channel_width"].sum(skipna=True)
            total_area = visit_channels["channel_area"].sum(skipna=True)

            weighted_vel = (
                visit_channels["channel_velocity"] * visit_channels["channel_area"]
            ).sum(skipna=True)
            avg_velocity = weighted_vel / total_area if total_area > 0 else None

            supplemental = {
                "channel-flow": (float(total_flow) if pd.notna(total_flow) else None),
                "top-width": (float(total_width) if pd.notna(total_width) else None),
                "main-channel-area": (
                    float(total_area) if pd.notna(total_area) else None
                ),
                "avg-velocity": (
                    float(avg_velocity) if avg_velocity is not None else None
                ),
            }

    # --- Top-level unit fields ---
    # area-unit and velocity-unit are only required when supplemental
    # values that use those units are present.
    has_area = supplemental.get("main-channel-area") is not None
    has_velocity = supplemental.get("avg-velocity") is not None

    json_data = {
        "height-unit": "ft",
        "flow-unit": "cfs",
        **({"area-unit": "ft2"} if has_area else {}),
        **({"velocity-unit": "ft/s"} if has_velocity else {}),
        "used": (bool(row["used"]) if pd.notna(row.get("used")) else False),
        "agency": (
            "USGS"
            if "unsp" in str(row.get("agency", "")).lower()
            else str(row.get("agency", "USGS"))
        ),
        "party": "",
        "wm-comments": wm_comments,
        "instant": instant_value,
        "id": {"office-id": str(row["office"]), "name": str(row["name"])},
        "number": str(row["number"]),
        "streamflow-measurement": {
            "gage-height": (
                float(row["gage-height"])
                if pd.notna(row.get("gage-height"))
                else CWMS_MISSING_VALUE
            ),
            "flow": (
                float(row["flow"]) if pd.notna(row.get("flow")) else CWMS_MISSING_VALUE
            ),
            "quality": str(row.get("quality", "")),
        },
        "usgs-measurement": {
            "remarks": remarks,
            "control-condition": (
                "Unspecified"
                if pd.notna(row.get("control-condition"))
                and "unsp" in str(row.get("control-condition", "")).lower()
                else (
                    str(row["control-condition"])
                    if pd.notna(row.get("control-condition"))
                    else None
                )
            ),
        },
    }

    if supplemental:
        json_data["supplemental-streamflow-measurement"] = supplemental

    json_data = remove_nan_values(json_data)
    return json_data


def realtime_mode(DAYS_BACK_MODIFIED, measurement_site_df):
    logger.info(
        f"Fetching USGS field measurements "
        f"(modified in last {DAYS_BACK_MODIFIED} days)..."
    )
    try:
        df_meas_usgs = fetch_field_measurements(
            parameter_code=["00060", "00065"],
            last_modified=f"PT{DAYS_BACK_MODIFIED * 24}H",
        )
    except Exception as e:
        logger.critical(f"Failed to fetch USGS measurements: {e}. Exiting.")
        exit(1)

    if df_meas_usgs.empty:
        logger.info("No new USGS measurements found to process.")
        exit(0)

    raw_record_count = len(df_meas_usgs)
    logger.info(f"Fetched {raw_record_count} raw USGS parameter records.")

    # --- Filter to CWMS-mapped sites before any further processing ---
    cwms_usgs_ids = set(f"USGS-{site}" for site in measurement_site_df["alias-id"])
    df_meas_usgs = df_meas_usgs[
        df_meas_usgs["monitoring_location_id"].isin(cwms_usgs_ids)
    ].copy()
    mapped_record_count = len(df_meas_usgs)
    logger.info(
        f"{mapped_record_count} records retained after CWMS mapping filter "
        f"({raw_record_count - mapped_record_count} records for unmapped sites dropped)."
    )

    if df_meas_usgs.empty:
        logger.info("No measurements for CWMS-mapped sites found. Exiting.")
        exit(0)

    # Fetch missing pair counterparts only for mapped sites
    df_meas_usgs = fetch_missing_pairs(df_meas_usgs)
    logger.info(
        f"After supplemental pair fetch: {len(df_meas_usgs)} records "
        f"({len(df_meas_usgs) - mapped_record_count} added)."
    )

    logger.info(f"Processing {len(df_meas_usgs) // 2} USGS measurements...")
    df_meas_usgs, dropped = process_usgs_data(df_meas_usgs)
    paired_count = len(df_meas_usgs)
    logger.info(
        f"Paired into {paired_count} flow/stage measurements "
        f"({dropped} rows dropped)."
    )

    df_channel = pd.DataFrame()
    if _fetch_channel:
        visit_ids = df_meas_usgs["usgs_field_visit_id"].unique().tolist()
        logger.info(
            f"Fetching channel measurements for {len(visit_ids)} unique field visits..."
        )
        df_channel = fetch_channel_measurements_chunked(visit_ids)
        logger.info(f"Fetched {len(df_channel)} channel measurement records.")
    else:
        logger.info("Channel measurement fetch skipped (--no-channel).")

    total_usgs_measurements_processed = 0
    total_usgs_measurements_skipped_no_cwms_mapping = 0
    office_store_stats = defaultdict(lambda: defaultdict(int))

    for _, usgs_row in df_meas_usgs.iterrows():
        total_usgs_measurements_processed += 1
        site_no = usgs_row.usgs_site_no

        site_filter_df = measurement_site_df[measurement_site_df["alias-id"] == site_no]
        if len(site_filter_df) == 0:
            total_usgs_measurements_skipped_no_cwms_mapping += 1
            continue

        cwms_loc = site_filter_df["location-id"].values[0]
        office_id = site_filter_df["office-id_x"].values[0]
        overwrite_flag = site_filter_df["attribute_x"].values[0]

        usgs_row_for_json = usgs_row.copy()
        usgs_row_for_json["name"] = cwms_loc
        usgs_row_for_json["office"] = office_id

        data = create_json_from_row(usgs_row_for_json, df_channel=df_channel)
        office_store_stats[office_id]["attempted"] += 1

        df_existing = pd.DataFrame()
        try:
            existing_measurements = cwms.get_measurements(
                location_id_mask=cwms_loc, office_id=office_id
            )
            if existing_measurements and existing_measurements.df is not None:
                df_existing = existing_measurements.df
        except Exception as e:
            logger.error(
                f"Error getting existing measurements for {cwms_loc} ({office_id}). "
                f"Assuming no existing measurements. Error: {e}"
            )

        _, is_rejected, df_differences = check_single_row_for_duplicates(
            usgs_row_for_json, df_existing
        )

        log_prefix = (
            f"USGS site {site_no} -> CWMS loc {cwms_loc} ({office_id}) "
            f"measurement collected at {usgs_row.get('instant')}"
        )

        if overwrite_flag == 1:
            try:
                logger.info(f"{log_prefix} (overwrite enabled). Storing.")
                cwms.store_measurements(data=[data], fail_if_exists=False)
                office_store_stats[office_id]["successful"] += 1
                if not df_differences.empty:
                    logger.info(
                        f"Differences found for {log_prefix}:\n"
                        f"{df_differences.to_string()}"
                    )
            except requests.exceptions.RequestException as e:
                logger.error(f"CWMS API network error storing {log_prefix}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error storing {log_prefix}: {e}")
        else:
            if not is_rejected:
                try:
                    logger.info(f"{log_prefix}. Storing.")
                    cwms.store_measurements(data=[data])
                    office_store_stats[office_id]["successful"] += 1
                    if not df_differences.empty:
                        logger.info(
                            f"Differences found for {log_prefix}:\n"
                            f"{df_differences.to_string()}"
                        )
                except requests.exceptions.RequestException as e:
                    logger.warning(
                        f"CWMS API network error (likely duplicate) storing "
                        f"{log_prefix}: {e}"
                    )
                    office_store_stats[office_id]["rejected"] += 1
                except Exception as e:
                    logger.error(f"Unexpected error storing {log_prefix}: {e}")
            else:
                logger.warning(
                    f"{log_prefix} has same number ({usgs_row.number}) or similar "
                    f"collection time as existing measurement. Not storing."
                )
                office_store_stats[office_id]["rejected"] += 1

    logger.info("-" * 50)
    logger.info("Processing Summary:")
    logger.info(f"Total raw USGS parameter records fetched: {raw_record_count}")
    logger.info(
        f"Records retained after CWMS mapping filter: {mapped_record_count} "
        f"({raw_record_count - mapped_record_count} dropped)"
    )
    logger.info(f"Total paired flow/stage measurements: {paired_count}")
    logger.info(
        f"Total unique measurements processed for CWMS store attempts: "
        f"{total_usgs_measurements_processed}"
    )
    logger.info(
        f"Total skipped (no CWMS mapping): "
        f"{total_usgs_measurements_skipped_no_cwms_mapping}"
    )

    logger.info("\nCWMS Store Statistics Per Office:")
    global_attempted = sum(stats["attempted"] for stats in office_store_stats.values())
    global_successful = sum(
        stats["successful"] for stats in office_store_stats.values()
    )
    global_rejected = sum(stats["rejected"] for stats in office_store_stats.values())

    for office, stats in sorted(office_store_stats.items()):
        logger.info(f"  Office: {office}")
        logger.info(f"    Attempted: {stats['attempted']}")
        logger.info(f"    Successful: {stats['successful']}")
        logger.info(f"    Rejected (Duplicate/Conflict): {stats['rejected']}")

    logger.info("\nOverall CWMS Store Statistics:")
    logger.info(f"Total CWMS store attempts: {global_attempted}")
    logger.info(f"Total CWMS stores successful: {global_successful}")
    logger.info(f"Total CWMS stores rejected (duplicate/conflict): {global_rejected}")
    logger.info("-" * 50)


def backfill_mode(BACKFILL_LIST, measurement_site_df):
    site_summary = {}
    overall_failed_stores = []

    for cwms_loc in BACKFILL_LIST:
        site_stats = {
            "measurements_fetched": 0,
            "measurements_saved": 0,
            "measurements_failed": 0,
            "failed_details": [],
        }

        site_filter_df = measurement_site_df[
            measurement_site_df["location-id"] == cwms_loc
        ]

        if site_filter_df.empty:
            logger.warning(
                f"CWMS location '{cwms_loc}' not found in measurement site list. "
                f"Skipping."
            )
            site_summary[cwms_loc] = site_stats
            continue

        usgs_site = site_filter_df["alias-id"].values[0]
        OFFICE = site_filter_df["office-id_x"].values[0]
        overwrite_code = int(site_filter_df["attribute_x"].values[0])

        logger.info(
            f"Fetching USGS POR field measurements for CWMS loc {cwms_loc} "
            f"(USGS {usgs_site})..."
        )

        por_start = "1850-01-01T00:00:00Z"
        try:
            df_meas_usgs = fetch_field_measurements(
                monitoring_location_id=f"USGS-{usgs_site}",
                parameter_code=["00060", "00065"],
                time=f"{por_start}/..",
            )
        except Exception as e:
            logger.critical(
                f"Failed to fetch USGS measurements for {cwms_loc} "
                f"(USGS {usgs_site}): {e}. Skipping site."
            )
            site_summary[cwms_loc] = site_stats
            continue

        if df_meas_usgs.empty:
            logger.info(f"No measurements found for {cwms_loc} (USGS {usgs_site}).")
            site_summary[cwms_loc] = site_stats
            continue

        raw_pair_estimate = len(df_meas_usgs) // 2
        logger.info(
            f"Processing {raw_pair_estimate} USGS measurements "
            f"for {cwms_loc} (USGS {usgs_site})..."
        )
        df_meas_usgs, dropped = process_usgs_data(df_meas_usgs)
        site_stats["measurements_fetched"] = len(df_meas_usgs)

        df_meas_usgs["location-id"] = df_meas_usgs["name"] = cwms_loc
        df_meas_usgs["office"] = OFFICE

        df_channel = pd.DataFrame()
        if _fetch_channel:
            visit_ids = df_meas_usgs["usgs_field_visit_id"].unique().tolist()
            logger.info(
                f"Fetching channel measurements for {len(visit_ids)} field visits "
                f"({cwms_loc})..."
            )
            df_channel = fetch_channel_measurements_chunked(visit_ids)
            logger.info(
                f"Fetched {len(df_channel)} channel measurement records for {cwms_loc}."
            )
        else:
            logger.info(
                f"Channel measurement fetch skipped for {cwms_loc} (--no-channel)."
            )

        log_prefix = (
            f"USGS site {usgs_site} -> CWMS loc {cwms_loc} ({OFFICE}) POR measurements"
        )

        df_existing = pd.DataFrame()
        try:
            existing_measurements = cwms.get_measurements(
                location_id_mask=cwms_loc, office_id=OFFICE
            )
            if existing_measurements and existing_measurements.df is not None:
                df_existing = existing_measurements.df
        except Exception as e:
            logger.error(
                f"Error getting existing measurements for {cwms_loc} ({OFFICE}). "
                f"Assuming none exist. Error: {e}"
            )

        if overwrite_code != 1:
            logger.info(
                "Overwrite flag is off. Filtering out conflicting measurements."
            )
            df_store, df_rejected_number, df_rejected_instant = (
                check_and_drop_duplicates(df_meas_usgs, df_existing)
            )
            if not df_rejected_number.empty:
                logger.info(
                    f"Rejected (duplicate number): "
                    f"{len(df_rejected_number)} measurements"
                )
            if not df_rejected_instant.empty:
                logger.info(
                    f"Rejected (within 5 min of existing): "
                    f"{len(df_rejected_instant)} measurements"
                )
        else:
            df_store = df_meas_usgs.copy()

        json_list = [
            create_json_from_row(row, df_channel=df_channel)
            for _, row in df_store.iterrows()
        ]

        try:
            logger.info(f"{log_prefix}. Storing {len(json_list)} measurements.")
            cwms.store_measurements(data=json_list, fail_if_exists=False)
            site_stats["measurements_saved"] = len(json_list)
        except requests.exceptions.RequestException as e:
            logger.error(f"CWMS API network error storing {log_prefix}: {e}")
            logger.info("Falling back to individual measurement storage...")
            site_stats["measurements_failed"] = len(json_list)
            for data in json_list:
                failure_detail = {
                    "site": f"{usgs_site} ({cwms_loc})",
                    "measurement_number": data.get("number", "Unknown"),
                    "instant": data.get("instant", "Unknown"),
                    "error": f"Network error: {e}",
                }
                site_stats["failed_details"].append(failure_detail)
                overall_failed_stores.append(failure_detail)
        except Exception as e:
            logger.error(f"Unexpected error storing {log_prefix}: {e}")
            logger.info("Storing one measurement at a time...")
            measurements_saved_individually = 0
            measurements_failed_individually = 0

            for data in json_list:
                try:
                    cwms.store_measurements(data=[data], fail_if_exists=False)
                    measurements_saved_individually += 1
                except Exception as individual_error:
                    measurements_failed_individually += 1
                    inst = data.get("instant", "Unknown")
                    number = data.get("number", "Unknown")
                    logger.error(
                        f"Could not store measurement {number} at {inst} "
                        f"for {cwms_loc}"
                    )
                    failure_detail = {
                        "site": f"{usgs_site} ({cwms_loc})",
                        "measurement_number": number,
                        "instant": inst,
                        "error": str(individual_error),
                    }
                    site_stats["failed_details"].append(failure_detail)
                    overall_failed_stores.append(failure_detail)

            site_stats["measurements_saved"] = measurements_saved_individually
            site_stats["measurements_failed"] = measurements_failed_individually

        site_summary[f"{usgs_site} ({cwms_loc})"] = site_stats
        logger.info(
            f"Site summary: fetched={site_stats['measurements_fetched']}, "
            f"saved={site_stats['measurements_saved']}, "
            f"failed={site_stats['measurements_failed']}"
        )

    logger.info("=" * 60)
    logger.info("OVERALL PROCESSING SUMMARY")
    logger.info("=" * 60)
    logger.info("MEASUREMENTS SAVED BY SITE:")
    logger.info("-" * 40)
    total_saved_all_sites = 0
    total_failed_all_sites = 0

    for site_name, stats in site_summary.items():
        logger.info(f"{site_name}:")
        logger.info(f"  - Fetched: {stats['measurements_fetched']}")
        logger.info(f"  - Saved: {stats['measurements_saved']}")
        logger.info(f"  - Failed: {stats['measurements_failed']}")
        total_saved_all_sites += stats["measurements_saved"]
        total_failed_all_sites += stats["measurements_failed"]
        logger.info("")

    logger.info(f"TOTAL MEASUREMENTS SAVED ACROSS ALL SITES: {total_saved_all_sites}")
    logger.info(f"TOTAL MEASUREMENTS FAILED ACROSS ALL SITES: {total_failed_all_sites}")

    if overall_failed_stores:
        logger.info("")
        logger.info("FAILED MEASUREMENT STORES SUMMARY:")
        logger.info("-" * 40)
        logger.info(f"Total failed measurements: {len(overall_failed_stores)}")

        failures_by_site = defaultdict(list)
        for failure in overall_failed_stores:
            failures_by_site[failure["site"]].append(failure)

        for site, failures in failures_by_site.items():
            logger.info(f"\n{site} - {len(failures)} failed measurements:")
            for failure in failures[:5]:
                logger.info(
                    f"  - Measurement {failure['measurement_number']} "
                    f"at {failure['instant']}"
                )
            if len(failures) > 5:
                logger.info(f"  - ... and {len(failures) - 5} more failures")
    else:
        logger.info("")
        logger.info("No failed measurement stores!")

    logger.info("=" * 60)


def getUSGS_measurement_cda(
    api_root,
    office_id,
    api_key,
    usgs_api_key=None,
    days_back_modified=2,
    backfill_list=None,
    backfill_group=None,
    fetch_channel=True,
):
    global _usgs_session, _fetch_channel

    # Configure USGS session once — api_key applied to every request automatically
    _usgs_session = requests.Session()
    if usgs_api_key:
        _usgs_session.params = {"api_key": usgs_api_key}
        logger.info("USGS API key configured.")
    else:
        logger.info("No USGS API key provided. Requests will be unauthenticated.")

    _fetch_channel = fetch_channel

    apiKey = "apikey " + api_key
    cwms.api.init_session(api_root=api_root, api_key=apiKey)

    logger.info("Fetching CWMS location groups...")
    try:
        usgs_alias_group = cwms.get_location_group(
            loc_group_id="USGS Station Number",
            category_id="Agency Aliases",
            office_id="CWMS",
            group_office_id=office_id,
            category_office_id=office_id,
        )
        usgs_measurement_locs = cwms.get_location_group(
            loc_group_id="USGS Measurements",
            category_id="Data Acquisition",
            office_id="CWMS",
            group_office_id=office_id,
            category_office_id=office_id,
        )
    except requests.exceptions.RequestException as e:
        logger.critical(f"Failed to fetch CWMS location groups: {e}. Exiting.")
        exit(1)
    except Exception as e:
        logger.critical(
            f"An unexpected error occurred fetching CWMS location groups: {e}. Exiting."
        )
        exit(1)

    measurement_site_df = pd.merge(
        usgs_measurement_locs.df,
        usgs_alias_group.df,
        on="location-id",
        how="inner",
    )
    measurement_site_df = measurement_site_df[measurement_site_df["alias-id"].notnull()]

    if measurement_site_df.empty:
        logger.warning("No valid USGS measurement locations found in CWMS. Exiting.")
        exit(0)

    if backfill_group:
        backfill_list = list(measurement_site_df["location-id"].values)

    if backfill_list:
        backfill_mode(backfill_list, measurement_site_df)
    else:
        realtime_mode(days_back_modified, measurement_site_df)


# --- Main Script Execution ---
def main():
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "-d",
        "--days_back_modified",
        default="2",
        help="Days back from current time measurements have been modified in USGS database.",
    )
    parser.add_argument(
        "-o",
        "--office",
        required=False,
        type=str,
        help="Office to grab data for. If not provided, will read from environment.",
    )
    parser.add_argument(
        "-a",
        "--api_root",
        required=False,
        type=str,
        help="API root for CDA. If not provided, will read from environment.",
    )
    parser.add_argument(
        "-k",
        "--api_key",
        default=None,
        type=str,
        help="CDA API key. If not provided, will read from CDA_API_KEY environment variable.",
    )
    parser.add_argument(
        "-u",
        "--usgs_api_key",
        default=None,
        type=str,
        help="USGS API key. If not provided, will read from USGS_API_KEY environment variable.",
    )
    parser.add_argument(
        "-b",
        "--backfill",
        default=None,
        type=str,
        help=(
            "Backfill POR data. Use a comma-separated list of CWMS location IDs "
            "(e.g. Dazey,Baldhill_Dam-Tailwater) or 'group' to backfill all sites "
            "in the office's Data Acquisition->USGS Measurements group."
        ),
    )
    parser.add_argument(
        "--channel",
        action=BooleanOptionalAction,
        default=True,
        help="Fetch and store USGS channel measurements in supplemental-streamflow-measurement. Use --no-channel to disable.",
    )

    args = parser.parse_args()
    DAYS_BACK_MODIFIED = int(args.days_back_modified)
    BACKFILL_GROUP = False
    BACKFILL_LIST = False
    if args.backfill is not None:
        if "group" in args.backfill:
            BACKFILL_GROUP = True
        elif type(args.backfill) == str:
            BACKFILL_LIST = args.backfill.replace(" ", "").split(",")

    CDA_API_ROOT = (
        args.api_root if args.api_root is not None else os.getenv("CDA_API_ROOT")
    )
    CDA_API_KEY = args.api_key if args.api_key is not None else os.getenv("CDA_API_KEY")
    OFFICE = args.office if args.office is not None else os.getenv("OFFICE")
    USGS_API_KEY = (
        args.usgs_api_key
        if args.usgs_api_key is not None
        else os.getenv("USGS_API_KEY")
    )

    if not CDA_API_ROOT:
        logger.critical(
            "CDA_API_ROOT not provided via argument or environment. Exiting."
        )
        exit(1)
    if not OFFICE:
        logger.critical("OFFICE not provided via argument or environment. Exiting.")
        exit(1)

    getUSGS_measurement_cda(
        api_root=CDA_API_ROOT,
        office_id=OFFICE,
        api_key=CDA_API_KEY,
        usgs_api_key=USGS_API_KEY,
        days_back_modified=DAYS_BACK_MODIFIED,
        backfill_list=BACKFILL_LIST,
        backfill_group=BACKFILL_GROUP,
        fetch_channel=args.channel,
    )


if __name__ == "__main__":
    main()
