import ujson as json
from datetime import *
import pandas as pd
from pyspark.sql.types import * #StructField, StructType

from moztelemetry import get_pings_properties
from moztelemetry.dataset import Dataset


class ColumnConfig:
    def __init__(self, name, path, cleaning_func, struct_type):
        self.name = name
        self.path = path
        self.cleaning_func = cleaning_func
        self.struct_type = struct_type

class DataFrameConfig:
    def __init__(self, col_configs):
        self.columns = [ColumnConfig(*col) for col in col_configs]

    def toStructType(self):
        return StructType(map(
            lambda col: StructField(col.name, col.struct_type, True),
            self.columns))

    def get_names(self):
        return map(lambda col: col.name, self.columns)

    def get_paths(self):
        return map(lambda col: col.path, self.columns)



def pings_to_df(sqlContext, pings, data_frame_config):
    """Performs simple data pipelining on raw pings

    Arguments:
        data_frame_config: a list of tuples of the form:
                 (name, path, cleaning_func, column_type)
    """
    def build_cell(ping, column_config):
        """Takes a json ping and a column config and returns a cleaned cell"""
        raw_value = ping[column_config.path]
        func = column_config.cleaning_func
        if func is not None:
            return func(raw_value)
        else:
            return raw_value

    def ping_to_row(ping):
        return [build_cell(ping, col) for col in data_frame_config.columns]

    filtered_pings = get_pings_properties(pings, data_frame_config.get_paths())

    return sqlContext.createDataFrame(
        filtered_pings.map(ping_to_row),
        schema = data_frame_config.toStructType())

def save_df(df, name, day):
    path_fmt = "s3n://telemetry-parquet/harter/cliqz_{name}/v1/submission={day}"
    path = path_fmt.format({'day':day, 'name':name})
    df.coalesce(1).write.mode("overwrite").parquet(path)

def __main__(sc, sqlContext):
    yesterday = (date.today() - timedelta(1)).strftime("%Y%m%d")
    get_doctype_pings = lambda docType: Dataset.from_source("telemetry") \
        .where(docType=docType) \
        .where(submissionDate=yesterday) \
        .where(appName="Firefox") \
        .records(sc)

    get_cliqz_version = lambda x: x["testpilot@cliqz.com"]["version"] if "testpilot@cliqz.com" in x.keys() else None

    testpilot_df = pings_to_df(
        sqlContext,
        get_doctype_pings("testpilot"),
        DataFrameConfig([
            ("client_id", "clientId", None, StringType()),
            ("submission_date", "meta/submissionDate", None, StringType()),
            ("creation_date", "creationDate", None, StringType()),
            ("geo", "meta/geoCountry", None, StringType()),
            ("locale", "environment/settings/locale", None, StringType()),
            ("channel", "meta/normalizedChannel", None, StringType()),
            ("os", "meta/os", None, StringType()),
            ("telemetry_enabled", "environment/settings/telemetryEnabled", None, BooleanType()),
            ("has_addon", "environment/addons/activeAddons", lambda x: "testpilot@cliqz.com" in x.keys(), StringType()),
            ("addons", "environment/addons/activeAddons", None, StringType()),
            ("cliqz_version", "environment/addons/activeAddons", get_cliqz_version, StringType()),
            ("event", "payload/events", lambda x: x[0]["event"], StringType()),
            ("events", "payload/events", None, ArrayType(MapType(StringType(), StringType())))
        ])
    )

    has_addon = lambda x: "testpilot@cliqz.com" in x.keys() if x is not None else None

    testpilottest_df = pings_to_df(
        sqlContext,
        get_doctype_pings("testpilottest"),
        DataFrameConfig([
            ("client_id", "clientId", None, StringType()),
            ("cliqz_client_id", "payload/payload/cliqzSession"), None, StringType()),
            ("session_id", "payload/payload/sessionId", None, StringType()),
            ("subsession_id", "payload/payload/subsessionId", None, StringType()),
            ("date", "meta/submissionDate", None, StringType()),
            ("client_timestamp", "creationDate", None, StringType()),
            ("geo", "meta/geoCountry", None, StringType()),
            ("locale", "environment/settings/locale", None, StringType()),
            ("channel", "meta/normalizedChannel", None, StringType()),
            ("os", "meta/os", None, StringType()),
            ("telemetry_enabled", "environment/settings/telemetryEnabled", None, StringType()),
            ("has_addon", "environment/addons/activeAddons", has_addon, StringType()),
            ("cliqz_version", "environment/addons/activeAddons", get_cliqz_version, None, StringType()),
            ("event", "payload/payload/event", None, StringType()),
            ("content_search_engine", "payload/payload/contentSearch", None, StringType())
        ])
    )

    return testpilot_df, testpilottest_df
