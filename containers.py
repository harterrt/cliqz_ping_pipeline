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

def save_df(df, name, date_partition, partitions=1):
    if date_partition is not None:
        partition_str = "/submission_date={day}".format(day=date_partition)
    else:
        partition_str=""

    # TODO: this name should include the experiment name
    path_fmt = "s3a://telemetry-parquet/harter/containers_{name}/v1{partition_str}"
    path = path_fmt.format(name=name, partition_str=partition_str)
    df.repartition(partitions).write.mode("overwrite").parquet(path)

def __main__(sc, sqlContext, day=None, save=True):
    if day is None:
        # Set day to yesterday
        day = (date.today() - timedelta(1)).strftime("%Y%m%d")

    get_doctype_pings = lambda docType: Dataset.from_source("telemetry") \
        .where(docType=docType) \
        .where(submissionDate=day) \
        .where(appName="Firefox") \
        .records(sc)

    testpilottest_df = pings_to_df(
        sqlContext,
        get_doctype_pings("testpilottest"),
        DataFrameConfig(
            [
                ("uuid", "payload/payload/uuid", None, StringType()),
                ("userContextId", "payload/payload/userContextId", None, LongType()),
                ("clickedContainerTabCount", "payload/payload/clickedContainerTabCount", None, LongType()),
                ("eventSource", "payload/payload/eventSource", None, StringType()),
                ("event", "payload/payload/event", None, StringType()),
                ("hiddenContainersCount", "payload/payload/hiddenContainersCount", None, LongType()),
                ("shownContainersCount", "payload/payload/shownContainersCount", None, LongType()),
                ("totalContainersCount", "payload/payload/totalContainersCount", None, LongType()),
                ("totalContainerTabsCount", "payload/payload/totalContainerTabsCount", None, LongType()),
                ("totalNonContainerTabsCount", "payload/payload/totalNonContainerTabsCount", None, LongType()),
                ("test", "payload/test", None, StringType()),
            ],
            lambda ping: ping['payload/test'] == "@testpilot-containers"
        )
    )

    if save:
        save_df(testpilottest_df, "testpilottest", day, partitions=16*5)

    return testpilottest_df
