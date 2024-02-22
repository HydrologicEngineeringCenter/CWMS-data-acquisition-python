from textfile import TextFile
from datetime import datetime


class CdaTextFile:
    """
    Process a text file that has multiple entries of time-series data
    """

    def __init__(self, filename):
        self.filename = filename

    def get_time_series_list(self):
        """
        parse out all time-series in filename
        :return: a list of time-series json objects
        """
        rval = []
        tf = TextFile(self.filename)
        print(f"read {len(tf)} lines")
        index_list = tf.find_all("Begin Header")
        end_index_list = index_list[1:]
        end_index_list = [n - 1 for n in end_index_list]
        end_index_list.append(len(tf) - 1)
        print(index_list)
        print(end_index_list)
        error_count = 0
        for i, idx in enumerate(index_list):
            tf1 = tf.subset_as_textfile(idx, end_index_list[i])
            ts = CdaTextFile.read_ts(tf1)
            size = len(ts["values"])
            if size > 0:
                rval.append(ts)
            else:
                print(f"no time-series data found {ts['name']}")
                error_count += 1
        print(f"There were {error_count} errors.")
        return rval

    @staticmethod
    def get_ts_values(tf: TextFile):
        rval = []
        idx1 = tf.find("End Header")
        if idx1 < 0:
            return rval
        idx1 += 2
        for i in range(idx1, len(tf)):
            line = tf.lines[i]
            dt_str, value = line.split(",")
            dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
            # dt = pytz.timezone('UTC').localize(dt)
            unix_epoch_ms = int(dt.timestamp() * 1000)
            # print(f"{unix_epoch_ms},{value}")
            rval.append([unix_epoch_ms, float(value), 0])  # time,value,flag=0

        return rval

    @staticmethod
    def read_ts(tf: TextFile):
        """ Reads a text file representation of a time-series into a Python dictionary
        :param tf: input TextFile with a single time-series
        :return: json representation of time-series
        """
        office = tf.get_value("office")
        timezone = tf.get_value("timezone")
        units = tf.get_value("units")
        tsid = tf.get_value("TimeSeriesID")
        if timezone.lower() != "gmt":
            raise ValueError(f"Unsupported timezone {timezone}.")

        values = CdaTextFile.get_ts_values(tf)
        print(f"{tsid} -> read {len(values)} values ")
        rval = {"name": tsid,
                "office-id": office,
                "units": units,
                "values": values
                }
        return rval
