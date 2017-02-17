# import ujson as json
# from datetime import *
# import pandas as pd
# from pyspark.sql.types import *
# from pyspark.sql.functions import split
# import base64
# from Crypto.Cipher import AES
# 
# from moztelemetry import get_pings_properties
# from moztelemetry.dataset import Dataset

def __main__(sc, sqlContext, day=None, save=True):
    if day is None:
        # Set day to yesterday
        day = (date.today() - timedelta(1)).strftime("%Y%m%d")

    get_doctype_pings = lambda docType: Dataset.from_source("telemetry") \
        .where(docType=docType) \
        .where(submissionDate=day) \
        .where(appName="Firefox") \
        .records(sc)

    testpilot_df = pings_to_df(
        sqlContext,
        get_doctype_pings("testpilot"),
        DataFrameConfig(
            [
                ("client_id", "clientId", None, StringType()),
                ("creation_date", "creationDate", None, StringType()),
                ("geo", "meta/geoCountry", None, StringType()),
                ("locale", "environment/settings/locale", None, StringType()),
                ("channel", "meta/normalizedChannel", None, StringType()),
                ("os", "meta/os", None, StringType()),
                ("telemetry_enabled", "environment/settings/telemetryEnabled", None, BooleanType()),
                ("has_addon", "environment/addons/activeAddons", has_addon, BooleanType()),
                ("cliqz_version", "environment/addons/activeAddons", get_cliqz_version, StringType()),
                ("event", "payload/events", get_event, StringType()),
                ("event_object", "payload/events", get_event_object, StringType()),
                ("test", "payload/test", None, StringType())
            ],
            lambda ping: ping['payload/test'] == '@testpilot-addon'
        )
    ).filter("event_object = 'testpilot@cliqz.com'")

    if save:
        save_df(testpilot_df, "testpilot", day)

    testpilottest_df = pings_to_df(
        sqlContext,
        get_doctype_pings("testpilottest"),
        DataFrameConfig(
            [
		("uuid", "payload.uuid", None, StringType()),
		("userContextId", "payload.userContextId", None, LongType()),
		("clickedContainerTabCount", "payload.clickedContainerTabCount", None, LongType()),
		("eventSource", "payload.eventSource", None, StringType()),
		("event", "payload.event", None, StringType()),
		("hiddenContainersCount", "payload.hiddenContainersCount", None, LongType()),
		("shownContainersCount", "payload.shownContainersCount", None, LongType()),
		("totalContainersCount", "payload.totalContainersCount", None, LongType()),
		("totalContainerTabsCount", "payload.totalContainerTabsCount", None, LongType()),
		("totalNonContainerTabsCount", "payload.totalNonContainerTabsCount", None, LongType())
            ],
            lambda ping: ping['payload/test'] == "testpilot@cliqz.com" # TODO FIX THIS FILTER
        )
    ).filter("event IS NOT NULL")

    if save:
        save_df(testpilottest_df, "testpilottest", day, partitions=16*5)

    return testpilot_df, testpilottest_df, search_df
