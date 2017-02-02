from datetime import *
from pyspark.sql.types import *

from moztelemetry import get_pings_properties
from moztelemetry.dataset import Dataset

from pyspark import SparkContext
from pyspark.sql import SQLContext

def init(old_sc):
    old_sc.stop()
    sc = SparkContext("local", "Cliqz Pipeline", pyFiles=['/home/hadoop/cliqz_ping_pipeline/transform.py'])
    sqlContext = SQLContext(sc)

    return sc, sqlContext

from transform import *

def __main__(sc, sqlContext):
    yesterday = (date.today() - timedelta(1)).strftime("%Y%m%d")
    get_doctype_pings = lambda docType: Dataset.from_source("telemetry") \
        .where(docType=docType) \
        .where(submissionDate=yesterday) \
        .where(appName="Firefox") \
        .records(sc)

    df_config = DataFrameConfig(
        sqlContext=sqlContext,
        col_configs = [
            ("client_id", "clientId", None, StringType()),
            ("submission_date", "meta/submissionDate", None, StringType()),
            ("creation_date", "creationDate", None, StringType()),
            #("events", "payload/events", None, StringType())
        ])

    return (get_doctype_pings, df_config)

def test(sc, sqlContext):
    print "run main"
    gdp, dfc = __main__(sc, sqlContext)
    print "get pings"
    pings = gdp("testpilot")
    print "build dataframe"
    df = pings_to_df(pings, dfc)
    print df.take(10)

    return (gdp, df, pings, df)

