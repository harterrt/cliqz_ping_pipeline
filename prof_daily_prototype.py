
# coding: utf-8

# In[81]:

sc.cancelAllJobs()


# ## Load and read parquet data

# In[51]:

sqlContext.read.parquet("s3://telemetry-parquet/main_summary/v3/")    .createOrReplaceTempView('main_summary')
ms_query = """
SELECT 
    client_id,
    submission_date,
    normalized_channel,
    os,
    is_default_browser,
    subsession_length,
    default_search_engine,
    search_counts
FROM main_summary
WHERE submission_date_s3 > '20170101'
"""


# ## Test query for one day of main_summary data
# 
# sqlContext.read.parquet("s3://telemetry-parquet/main_summary/v3/submission_date_s3=20170211/")\
#     .createOrReplaceTempView('main_summary')
# ms_query = """
# SELECT 
#     client_id,
#     submission_date,
#     normalized_channel,
#     os,
#     is_default_browser,
#     subsession_length,
#     default_search_engine,
#     search_counts
# FROM main_summary
# """

# In[52]:

sqlContext.read.parquet("s3://telemetry-parquet/harter/cliqz_testpilot/v1/").createOrReplaceTempView('cliqz_testpilot')
sqlContext.read.parquet("s3://telemetry-parquet/harter/cliqz_testpilottest/v1/").createOrReplaceTempView('cliqz_testpilottest')


# In[76]:

ms = sqlContext.sql(ms_query)

txp_min_query = """
SELECT tp.client_id, min(date) as min_date
FROM cliqz_testpilot tp
JOIN cliqz_testpilottest tpt
ON tpt.client_id = tp.client_id
GROUP BY 1
"""

txp_min = sqlContext.sql(txp_min_query)

txp_query = """
SELECT 
    tp.client_id,
    tpt.cliqz_client_id,
    tp.submission as submission_date,
    tp.cliqz_version,
    tp.has_addon,
    tp.cliqz_version,
    tpt.event,
    tpt.content_search_engine
FROM cliqz_testpilot tp
JOIN cliqz_testpilottest tpt
ON tpt.client_id = tp.client_id
AND tpt.submission == tp.submission
"""
txp = sqlContext.sql(txp_query)


# In[54]:

#txp_min.take(10)


# In[55]:

#txp.take(10)


# In[56]:

#ms.take(10)


# ## Filter to two week window of main summary
# (this does nothing for now because we are only using one day of main summary data)

# In[57]:

from datetime import datetime, timedelta
def filter_two_week_delta(row):
    try:
        delta = (datetime.strptime(row.min_date, "%Y%m%d") - datetime.strptime(row.submission_date, "%Y%m%d"))
        return delta <= timedelta(14)
    except:
        return False

filtered_ms = txp_min.join(ms, [txp_min.client_id == ms.client_id], "left_outer").rdd    .filter(filter_two_week_delta)


# In[58]:

#filtered_ms.take(10)


# ## Aggregate
# 
# This aggregation is pretty messy. 
# We effectively take an arbitrary value for anything not included in the Counter object.

# In[59]:

from collections import Counter
def agg_func(x, y):
    return x[0], x[1] + y[1]

def prep_ms_agg(row):
    def parse_search_counts(search_counts):
        if search_counts is not None:
            return Counter({(xx.engine, xx.source): xx['count'] for xx in search_counts})
        else:
            return Counter()

    return ((row.client_id, row.submission_date), (
        row,
        Counter({
            "is_default_browser_counter": Counter([row.is_default_browser]),
            "session_hours": float(row.subsession_length if row.subsession_length else 0)/3600,
            "search_counts": parse_search_counts(row.search_counts)
        })
    ))

def prep_txp_agg(row):
    return ((row.client_id, row.submission_date), (
        row,
        Counter({
            "cliqz_enabled": int(row.event == "enabled"),
            "cliqz_enabled": int(row.event == "disabled"),
            "test_enabled": int(row.event == "cliqzEnabled"),
            "test_disabled": int(row.event == "cliqzDisabled"),
            "test_installed": int(row.event == "cliqzInstalled"),
            "test_uninstalled": int(row.event == "cliqzUninstalled")
        })
    ))


# In[60]:

agg_ms = filtered_ms.map(prep_ms_agg).reduceByKey(agg_func)


# In[61]:

agg_txp = txp.rdd.map(prep_txp_agg).reduceByKey(agg_func)


# ## Join aggregated tables

# In[62]:

joined = agg_ms.fullOuterJoin(agg_txp)


# In[63]:

from pyspark.sql import Row
profile_daily = Row('client_id', 'cliqz_client_id', 'date', 'has_cliqz',
                    'cliqz_version', 'channel', 'os', 'is_default_browser',
                    'session_hours', 'search_default', 'search_counts',
                    'cliqz_enabled', 'cliqz_disabled', 'test_enabled',
                    'test_disabled', 'test_installed', 'test_uninstalled')

def option(value):
    return lambda func: func(value) if value is not None else None

def format_row(row):
    print(row)
    key = row[0]
    value = row[1]
    
    main_summary = option(value[0][0] if value[0] is not None else None)
    ms_agg = option(value[0][1] if value[0] is not None else None)
    testpilot = option(value[1][0] if value[1] is not None else None)
    txp_agg = option(value[1][1] if value[1] is not None else None)

    search_counts = ms_agg(lambda x:x['search_counts'])
    
    return Row(
        client_id = key[0],
        cliqz_client_id = testpilot(lambda x: x.cliqz_client_id),
        date = key[1],
        has_cliqz = testpilot(lambda x: x.has_addon),
        cliqz_version = testpilot(lambda x: x.cliqz_version),
        channel = main_summary(lambda x: x.normalized_channel),
        os = main_summary(lambda x: x.os),
        is_default_browser = ms_agg(lambda x: x['is_default_browser_counter'].most_common()[0][0]),
        session_hours = ms_agg(lambda x: x['session_hours']),
        search_default = main_summary(lambda x: x.default_search_engine),
        search_counts = search_counts if search_counts is not None else {},
        cliqz_enabled = txp_agg(lambda x: x['cliqz_enabled']),
        cliqz_disabled = txp_agg(lambda x: x['cliqz_enabled']),
        test_enabled = txp_agg(lambda x: x['test_enabled']),
        test_disabled = txp_agg(lambda x: x['test_disabled']),
        test_installed = txp_agg(lambda x: x['test_installed']),
        test_uninstalled = txp_agg(lambda x: x['test_uninstalled'])
    )


# In[64]:

final = joined.map(format_row)


# In[ ]:




# In[65]:

# sqlContext.createDataFrame(final)


# In[66]:

#txp.filter("submission_date = 20170211").count()


# In[67]:

#agg_txp.count()


# In[68]:

#agg_ms.count()


# In[69]:

#agg_ms.map(lambda x: x[1][0]['client_id']).distinct().count()


# In[70]:

#agg_txp.count() + agg_ms.count() - final.count() 


# In[71]:

local_final = sqlContext.createDataFrame(final.collect()).coalesce(1).write.mode("overwrite")    .parquet("s3n://telemetry-parquet/harter/cliqz_profile_daily/v1/")


# # Optional Struct Type
# struct = StructType([
#     StructField('client_id', StringType(), True),
#     StructField('cliqz_client_id', StringType(), True),
#     StructField('date', StringType(), True),
#     StructField('has_cliqz', BooleanType(), True),
#     StructField('cliqz_version', StringType(), True),
#     StructField('channel', StringType(), True),
#     StructField('os', StringType(), True),
#     StructField('is_default_browser', BooleanType(), True),
#     StructField('session_hours', DoubleType(), True),
#     StructField('search_default', StringType(), True),
#     StructField('search_counts', MapType(), True),
#     StructField('cliqz_enabled', IntegerType(), True),
#     StructField('cliqz_disabled', IntegerType(), True),
#     StructField('test_enabled', IntegerType(), True),
#     StructField('test_disabled', IntegerType(), True),
#     StructField('test_installed', IntegerType(), True),
#     StructField('test_uninstalled'), IntegerType(), True),
# ])
