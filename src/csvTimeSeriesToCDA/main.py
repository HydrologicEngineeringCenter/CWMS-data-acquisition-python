# this program reads a text file that contains time-series data
# the time-series data is written to the CDA web service.

# Required Environment Variables (by example)
# CDA_KEY=sessionkey-for-testing
# CDA_SERVICE=https://cwms-data.test:8444

# example input file
# Begin Header
# # description of data source
# # source: servername.domain.ext
# office=SAS
# timezone=GMT
# TimeSeriesID=Hartwell-Powerhouse-Unit5.Flow.Ave.1Hour.1Hour.Raw-test-data
# End Header
# Date Time,Elevation
# 2024-01-02T21:00:00,0.00
# 2024-01-02T22:00:00,0.00
# 2024-01-02T23:00:00,0.00
# 2024-01-03T00:00:00,0.00
# 2024-01-03T01:00:00,5.36
# 2024-01-03T02:00:00,2887.82
# 2024-01-03T03:00:00,5572.70
# 2024-01-03T04:00:00,2826.93
# 2024-01-03T05:00:00,5.13

import os

from CdaTextFile import CdaTextFile
import http.client
import json
import ssl
from urllib.parse import urlparse
import sys


def write_ts_to_cda(ts):
    """
    Writes dictionary ts to endpoint define in CDA_SERVICE
    TO DO: - use https://github.com/HydrologicEngineeringCenter/cwms-python for writing to the API ?
    :param ts:
    :return:
    """
    print(f"saving to CDA: {ts['name']}")
    raw_host = os.getenv("CDA_SERVICE")
    if raw_host is None:
        raise Exception("environment variable for endpoint is not set 'CDA_SERVICE'")
    cda_api_key = os.getenv("CDA_API_KEY")
    if cda_api_key is None:
        raise Exception("environment variable for key is not set 'CDA_KEY'")
    print(f"connecting to '{raw_host}'")
    parsed_host = urlparse(raw_host)

    hostname = parsed_host.hostname
    if hostname == "cwms-data.test":
        context = ssl._create_unverified_context()
    else:
        context = None

    conn = http.client.HTTPSConnection(hostname, port=parsed_host.port, context=context)
    headers = {
        'accept': '*/*',
        'Content-Type': 'application/json;version=2',
        'Authorization': 'apikey ' + cda_api_key
    }

    body = json.dumps(ts)
    print(body)
    conn.request("POST", "/cwms-data/timeseries", body=body, headers=headers)
    response = conn.getresponse()
    if response.status == 200:
        data = response.read()
        print(f"response=OK {data.decode('utf-8')}")
    else:
        print(f'Request failed with status code: {response.status}')
        print("Headers:", response.getheaders())
        error_data = response.read().decode("utf-8")
        print("Response body:", error_data)


if __name__ == '__main__':
    dry_run = False
    if len(sys.argv) >= 2:
        filename = sys.argv[1]
        print(f"The provided filename is: {filename}")
        if len(sys.argv) == 3:
            dry_run = sys.argv[2]
    else:
        print("No filename provided.")
        print("Usage main.py input_filename.txt [dry-run]")
        print("Where:")
        print("        input_filename.txt is input time-series data")
        print("        dry-run - if provided as the second argument , "
              " parse input_filename.txt without saving to the endpoint")
        exit(-1)

    cda_file = CdaTextFile(filename)
    series_list = cda_file.get_time_series_list()

    if not dry_run:
        for ts in series_list:
            write_ts_to_cda(ts)
