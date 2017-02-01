import ujson as json
from datetime import *
import pandas as pd

from moztelemetry import get_pings_properties
from moztelemetry.dataset import Dataset

get_doctype_pings = lambda docType: Dataset.from_source("telemetry") \
    .where(docType=docType) \
    .where(submissionDate=yesterday) \
    .where(appName="Firefox") \
    .records(sc)

columns = [
    ("client_id", "client_id", None, StringType()),
    ("submission_date", "meta/submissionDate", None, StringType()),
    ("creation_date", "creation_date", None, StringType()),
    ("events", "payload/events", None, StringType())
]

def pings_to_df(pings, columns):
    """Performs simple data pipelining on raw pings

    Arguments:
        columns: a list of tuples of the form:
                 (name, path, cleaning_func, column_type)
    """
    def build_cell(ping, column):
        """Takes a json ping and a column config and returns a cleaned cell"""
        raw_value = ping[column[1]]
        func = column[2]
        if func is not None:
            return func(raw_value)
        else:
            return raw_value

    def ping_to_row(ping):
        return [build_cell(ping, col) for col in columns]

    filtered_pings = get_pings_properties(pings, config.map(lambda x: x[1]))
    filtered_pings.map(ping_to_row)
