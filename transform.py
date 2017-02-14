import ujson as json
from datetime import *
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import split
import base64
from Crypto.Cipher import AES

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
    filtered_pings = get_pings_properties(pings, data_frame_config.get_paths())

    return config_to_df(sqlContext, filtered_pings, data_frame_config)

def config_to_df(sqlContext, raw_data, data_frame_config):
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

    return sqlContext.createDataFrame(
        filtered_pings.map(ping_to_row),
        schema = data_frame_config.toStructType())

def save_df(df, name, date_partition, partitions=1):
    if date_partition is not None:
        partition_str = "/submission={day}".format(day=date_partition)
    else:
        partition_str=""


    # Choose cliqz_{name} over cliqz/{name} b.c. path now matches presto table name
    path_fmt = "s3n://telemetry-parquet/harter/cliqz_{name}/v1{partition_str}"
    path = path_fmt.format(name=name, partition_str=partition_str)
    df.coalesce(partitions).write.mode("overwrite").parquet(path)

def __main__(sc, sqlContext, day=None, save=True):
    if day is None:
        # Set day to yesterday
        day = (date.today() - timedelta(1)).strftime("%Y%m%d")

    get_doctype_pings = lambda docType: Dataset.from_source("telemetry") \
        .where(docType=docType) \
        .where(submissionDate=day) \
        .where(appName="Firefox") \
        .records(sc)

    KEY = sc.textFile("s3://telemetry-parquet/harter/cliqz_key").take(1)[0]
    def decrypt_cliqz_id(cliqz_id, key=KEY):
        if cliqz_id is not None:
            secret = AES.new(key)
            try:
                return secret.decrypt(base64.b64decode(cliqz_id)).rstrip("\0")
            except:
                return None
        else:
            return None

    def split_cliqz_id(cliqz_id):
        decrypted = decrypt_cliqz_id(cliqz_id)
        return decrypted.split("|")[0] if decrypted is not None else None

    get_cliqz_version = lambda x: x["testpilot@cliqz.com"]["version"] if x is not None and "testpilot@cliqz.com" in x.keys() else None
    has_addon = lambda x: "testpilot@cliqz.com" in x.keys() if x is not None else None
    get_event = lambda x: x[0]["event"] if x is not None else None
    get_event_object = lambda x: x[0]["object"] if x is not None else None

    testpilot_df = pings_to_df(
        sqlContext,
        get_doctype_pings("testpilot"),
        DataFrameConfig([
            ("client_id", "clientId", None, StringType()),
            ("creation_date", "creationDate", None, StringType()),
            ("geo", "meta/geoCountry", None, StringType()),
            ("locale", "environment/settings/locale", None, StringType()),
            ("channel", "meta/normalizedChannel", None, StringType()),
            ("os", "meta/os", None, StringType()),
            ("telemetry_enabled", "environment/settings/telemetryEnabled", None, BooleanType()),
            ("has_addon", "environment/addons/activeAddons", has_addon, StringType()),
            ("cliqz_version", "environment/addons/activeAddons", get_cliqz_version, StringType()),
            ("event", "payload/events", get_event, StringType()),
            ("event_object", "payload/events", get_event_object, StringType()),
            ("test", "payload/test", None, StringType())
        ])).filter("test = '@testpilot-addon'") \
           .filter("event_object = 'testpilot@cliqz.com'")

    if save:
        save_df(testpilot_df, "testpilot", day)

    testpilottest_df = pings_to_df(
        sqlContext,
        get_doctype_pings("testpilottest"),
        DataFrameConfig([
            ("client_id", "clientId", None, StringType()),
            ("enc_cliqz_udid", "payload/payload/cliqzSession", None, StringType()),
            ("cliqz_udid", "payload/payload/cliqzSession", decrypt_cliqz_id, StringType()),
            ("cliqz_client_id", "payload/payload/cliqzSession", split_cliqz_id, StringType()),
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
            ("cliqz_version", "environment/addons/activeAddons", get_cliqz_version, StringType()),
            ("event", "payload/payload/event", None, StringType()),
            ("content_search_engine", "payload/payload/contentSearch", None, StringType()),
            ("test", "payload/test", None, StringType())
        ])).filter("event IS NOT NULL") \
           .filter("test = 'testpilot@cliqz.com'")

    if save:
        save_df(testpilottest_df, "testpilottest", day, partitions=16*5)

    search_df = config_to_df(
        sqlContext,
        sqlContext.read.options(header=True) \
            .csv("s3://net-mozaws-prod-cliqz/testpilot-cliqz-telemetry.csv"),
        DataFrameConfig([
            ("client_id_cliqz", "udid", lambda x: x.split('\|')[0], StringType()),
            ("date", "start_time", None, StringType()),
            ("is_search", "selection_type", lambda x: x in ["query", "enter", "click"], BooleanType()),
            ("entry_point", "entry_point", None, StringType()),
            ("num_cliqz_results_shown", "final_result_list_backend_result_count", None, IntegerType()),
            ("were_browser_results_shown", "final_result_list_contains_history", None, BooleanType()),
            ("final_query_length", "selection_query_length", None, IntegerType()),
            ("landing_type", "selection_type", None, StringType()),
            ("landing_rich_type", "selection_class", None, StringType()),
            ("landed_on_inner_link", "selection_element", None, BooleanType()),
            ("landing_position", "selection_index", None, IntegerType()),
            ("autocompleted", "selection_type", lambda x: x == "autocomplete", BooleanType()),
            ("navigated_to_search_page", "selection_type", lambda x: x == "query", BooleanType()),
            ("count", "total_signal_count", None, IntegerType()),
            ("selection_time", "selection_time", None, IntegerType()),
            ("final_result_list_show_time", "final_result_list_show_time", None, IntegerType()),
            ("selection_source", "selection_source", None, StringType())
        ])
    )

    if save:
        save_df(search_df, "search", None)

    return testpilot_df, testpilottest_df, search_df
