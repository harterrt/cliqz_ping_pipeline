import ujson as json
from datetime import *
import pandas as pd
from pyspark.sql.types import StructField, StructType

from moztelemetry import get_pings_properties
from moztelemetry.dataset import Dataset


class DataFrameConfig:
    class ColumnConfig:
        def __init__(self, name, path, cleaning_func, struct_type):
            self.name = name
            self.path = path
            self.cleaning_func = cleaning_func
            self.struct_type = struct_type

    def __init__(self, col_configs):
        self.columns = [ColumnConfig(*col) for col in col_configs]

    def toStructType(self):
        StructType(self.columns.map(
            lambda col: StructField(col.name, col.struct_type, True)))


def pings_to_df(pings, data_frame_config):
    """Performs simple data pipelining on raw pings

    Arguments:
        data_frame_config: a list of tuples of the form:
                 (name, path, cleaning_func, column_type)
    """
    def build_cell(ping, column_config):
        """Takes a json ping and a column config and returns a cleaned cell"""
        raw_value = ping[column_config.path]
        func = column.cleaning_func
        if func is not None:
            return func(raw_value)
        else:
            return raw_value

    def ping_to_row(ping):
        return [build_cell(ping, col) for col in data_frame_config.columns]

    filtered_pings = get_pings_properties(pings, config.map(lambda x: x[1]))

    return sqlContext.createDataFrame(
        filtered_pings.map(ping_to_row),
        data_frame_config.toStructType)

def __main__():
    get_doctype_pings = lambda docType: Dataset.from_source("telemetry") \
        .where(docType=docType) \
        .where(submissionDate=yesterday) \
        .where(appName="Firefox") \
        .records(sc)

    df_config = DataFrameConfig([
        ("client_id", "client_id", None, StringType()),
        ("submission_date", "meta/submissionDate", None, StringType()),
        ("creation_date", "creation_date", None, StringType()),
        ("events", "payload/events", None, StringType())
    ])

    return (get_doctpe_pings, df_config)


