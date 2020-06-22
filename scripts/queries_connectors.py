from connections import athena_connection,postprocessing_connection
import pandas as pd
import logging
from datetime import datetime, timezone
from dateutil.relativedelta import *


SPARK_NAMES = [
    'mongo-java-driver|mongo-spark',
    'mongo-java-driver|legacy|mongo-spark'
]

KAFKA_NAMES = [
    'mongo-java-driver|sync|mongo-kafka|sink',
    'mongo-java-driver|sync|mongo-kafka|source',
    'mongo-java-driver|sync|mongo-kafka'
]

BIC_NAMES = [
    'mongosqld',
    'mongodrdl'
]

SOURCE_TABLE='cloud_backend_raw.dw__cloud_backend__rawclientmetadata'

def driver_names_joined(names_list):
    return "'" + "','".join(names_list)+"'"

def last_six_months_start_and_end_date():
    today = datetime.today()
    first_day_of_this_month = \
    datetime(today.year, today.month, 1,tzinfo=timezone.utc)
    six_months_ago = first_day_of_this_month + relativedelta(months=-6)
    six_months_ago = six_months_ago.strftime('%Y-%m-%d')
    first_day_of_this_month = first_day_of_this_month.strftime('%Y-%m-%d')
    return  (six_months_ago,first_day_of_this_month)

def query_new_vs_existing_6_months(names_list):
    start_date,end_date = last_six_months_start_and_end_date()
    query = """
    WITH min_dates_by_month as (\
    select gid__oid,\
   month(rt) as month,\
   year(rt) as year,\
   min(rt) as min_rt_month\
   from {0} where \
   entries__raw__driver__name in ({1})\
               and date(rt) >= date '{2}' and processed_date >= '{2}'\
               and date(rt) < date '{3}' and processed_date <= '{3}'\
               group by month(rt), year(rt), gid__oid\
 ),\
  min_dates_overall as (\
    select gid__oid,\
    min(rt) as min_rt from {0} where entries__raw__driver__name in ({1})\
    group by gid__oid\
    )\
   select\
   count_if(status = 'new') as new_projects,\
   count_if(status = 'existing') as existing_projects,\
   count(distinct group_id) as count_projects,\
   min(min_rt_month) as ts,\
   month,\
   year\
   from (\
   select min_dates_by_month.gid__oid as group_id,\
   min_rt_month, month, year, min_rt,\
   CASE WHEN min_rt_month > min_rt THEN 'existing' ELSE 'new' END as status\
   from\
   min_dates_by_month join min_dates_overall on min_dates_by_month.gid__oid = min_dates_overall.gid__oid)\
   group by month, year\
    """.format(SOURCE_TABLE,names_list,start_date, end_date)
    print(query)
    return query

def query_spark_new_vs_existing_6_months():
    query = query_new_vs_existing_6_months(driver_names_joined(SPARK_NAMES))
    return query

def query_kafka_new_vs_existing_6_months():
    query = query_new_vs_existing_6_months(driver_names_joined(KAFKA_NAMES))
    return query

def query_bic_clusters():
    query = """
        WITH start_of_time as \
        (select date_add('day',-(day_of_week(current_date)-1)-56,current_date) as day)\

         SELECT week, \
           COUNT(DISTINCT CASE \
                  WHEN bi_connector=true\
                 THEN cluster_id ELSE NULL END)as clusters_with_bi_connector,\
           cast(date_trunc('week',date) as timestamp) as date,\
           ca_instance_size as instance_size\
            from ns__data_analyst_internal.natalya_atlas_clusters_hist\
             where date >= (select day from start_of_time limit 1)\
            group by week, date_trunc('week',date), ca_instance_size\
            order by week, date_trunc('week',date), ca_instance_size
    """
    print(query)
    return query

def query_connectors_weekly(names_list):
    query ="""
        WITH start_of_time as (select date_add('day',-(day_of_week(current_date)-1)-56,current_date) as day)\
        select count(distinct group_id) as groups_count, max(ts) as max_ts,min(ts) as min_ts, week\
        FROM \
     (SELECT \
     date_trunc('week',rt) as week,\
     rt as ts,\
     gid__oid as group_id\
     from \
     {0} where entries__raw__driver__name in \
     ({1})\
      and date(rt) >= (select day from start_of_time limit 1)\
     )\
     group by week\
     order by week
    """.format(SOURCE_TABLE,names_list)
    print(query)
    return query

def query_bic_weekly():
    query ="""
        WITH start_of_time as (select date_add('day',-(day_of_week(current_date)-1)-56,current_date) as day)\
        select count(distinct group_id) as groups_count, max(ts) as max_ts,min(ts) as min_ts, week\
        FROM \
     (SELECT \
     date_trunc('week',rt) as week,\
     rt as ts,\
     gid__oid as group_id\
     from \
     {0} where entries__raw__application__name in \
     ({1})\
      and date(rt) >= (select day from start_of_time limit 1)\
     )\
     group by week\
     order by week
    """.format(SOURCE_TABLE,driver_names_joined(BIC_NAMES))
    print(query)
    return query

def query_spark_weekly():
    query = query_connectors_weekly(driver_names_joined(SPARK_NAMES))
    return query

def query_kafka_weekly():
    query = query_connectors_weekly(driver_names_joined(KAFKA_NAMES))
    return query

def query_kafka_source_vs_sink_monthly():
    query = """
        SELECT count(distinct gid__oid) as count_groups,\
        month(rt) as month, year(rt) as year, min(rt) as min_ts,\
        max(rt) as max_ts,\
        entries__raw__driver__name as driver_name\
        from {0} \
        where entries__raw__driver__name in ({1})\
        and processed_date >= '2020-06-01' and rt >= date '2020-06-01'\ # start of tracking this
        group by entries__raw__driver__name, year(rt), month(rt)\
        order by year, month\
    """.format(SOURCE_TABLE,driver_names_joined(KAFKA_NAMES))
    return query

collections_queries = [
    {'spark_monthly_new_vs_existing': query_spark_new_vs_existing_6_months()},
    {'kafka_monthly_new_vs_existing': query_kafka_new_vs_existing_6_months()},
    {'bic_active_clusters': query_bic_clusters()},
    {'bic_usage_weekly': query_bic_weekly()},
    {'kafka_weekly': query_kafka_weekly()},
    {'spark_weekly': query_spark_weekly()},
    {'kafka_sink_source_monthly': query_kafka_source_vs_sink_monthly()}
]

def run_query(query):
    conn = athena_connection()
    try:
        result = pd.read_sql(query,conn)
        result = result.to_dict('records')
        return result
    except Exception as x:
        print(x)
        logging.error(x)
        raise
    finally:
        print("closing connection")
        conn.close()
