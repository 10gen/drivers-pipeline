#!/usr/bin/env python

import argparse
import os
import pandas as pd
import numpy as np
import pymongo
import dns
from bson.objectid import ObjectId
import datetime
import pprint
from datetime import timezone, datetime, tzinfo, date,time, timedelta
import pdb
import logging
import time
from pyathenajdbc import connect
from query import *
from helpers import function_with_time_elapsed, max_available_date, query_delete_many
from connections import athena_connection, postprocessing_connection

#root_dir = os.path.abspath(os.path.join(__file__, '../..'))
logging.basicConfig(filename='drivers_metrics.log',level=logging.INFO)

MONTHLY_COLLECTION = 'drivers_monthly'
LATEST_MONTH_COLLECTION = 'drivers_latest_month'

"""
Method finds default start and end dates, which are the beginning of last month
and the beginning of this month
"""
def default_start_and_end_date():
    today = datetime.today()
    first_day_of_this_month = \
    datetime(today.year, today.month, 1,tzinfo=timezone.utc)
    last_month_date = first_day_of_this_month - timedelta(days = 1)
    first_day_of_last_month = \
    datetime(last_month_date.year, last_month_date.month, 1,tzinfo=timezone.utc)
    str_first_day_of_this_month = first_day_of_this_month.strftime('%Y-%m-%d')
    str_first_day_of_last_month = first_day_of_last_month.strftime('%Y-%m-%d')
    return (str_first_day_of_last_month,str_first_day_of_this_month)

"""
Query Athena to aggregate raw data and return a list of dictionaries
query - a query to be run in Athena
"""
def monthly_aggregation_from_athena(query):
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

"""
Processes a document by parsing "platform" field and depending on the driver,
extract language version, framework (where exists) and provider, and convert
group IDs to ObjectID for loading into Mongo.
doc - a dictionary as returned by the Athena query after converting rows to
list of dictionaries.
"""
def transform(doc):
    language_version = None
    framework = None
    provider = None
    driver_name = doc['d']
    doc['gid'] = ObjectId(doc['gid'])
    if 'p' in doc:
        try:
            platform_name = doc['p']
            if driver_name in python_driver_names():
                language_version,framework = parse_python(platform_name)
            elif driver_name in node_driver_names():
                language_version = parse_node(platform_name)
            elif driver_name in go_driver_names():
                language_version = parse_go(platform_name)
            elif driver_name in java_driver_names():
                provider,language_version = parse_java(platform_name,driver_name)
            elif driver_name in ruby_driver_names():
                framework, language_version = parse_ruby(platform_name)
            elif driver_name in perl_driver_names():
                language_version = parse_perl(platform_name)
            elif driver_name in csharp_driver_names():
                framework = parse_csharp(platform_name)
            elif driver_name in other_drivers_names(): # drivers without current need for versions parsing
                pass
            if language_version is not None:
                doc['lver'] = language_version
            if framework is not None:
                doc['fr'] = framework
            if provider is not None:
                doc['prov'] = provider
        except Exception as e:
            #logging.error("Exception %s for doc with platform string %s",e,platform_name)
            print("Exception '{0}' for doc with platform string {1} and driver_name {2}".format(e,platform_name,driver_name))
    else:
        logging.info("Document with driver name {0} did not have platform field".format(driver_name))
    return doc

"""
Transforms list of documents by applying transform() method to each.
docs - list of dictionaries
"""
@function_with_time_elapsed("Transform took: ")
def transform_list(docs):
    result = list(map(lambda x: transform(x), docs))
    return result

"""
Checks if data exists for given date range and deletes it from historical
monthly collection (ensuring idempotency).
"""
def check_if_data_exists_for_range_of_dates(start_date,end_date):
    print("Checking if data exists for the period.")
    conn=postprocessing_connection()
    db = conn.drivers
    collection_monthly = db[MONTHLY_COLLECTION]
    collection_latest_month = db[LATEST_MONTH_COLLECTION]
    try:
        latest_date = max_available_date(collection_monthly)
        if (latest_date and (latest_date >= datetime.strptime(start_date, "%Y-%m-%d") and \
        latest_date < datetime.strptime(end_date, "%Y-%m-%d")) ):
            print("Data already loaded for the time period. Deleting it.")
            query_delete_many(start_date,end_date,collection_monthly)
            query_delete_many(start_date,end_date,collection_latest_month)
    except Exception as e:
        print(e)
        raise
    finally:
        conn.close()

"""
Extract step of the ETL.
"""
@function_with_time_elapsed("Extract took: ")
def extract(start_date,end_date):
    query = query_drivers(start_date,end_date)
    print(query)
    docs = monthly_aggregation_from_athena(query)
    return docs

"""
Load step of the ETL. Loads data into 2 collection: latest month and historical
monthly.
"""
@function_with_time_elapsed("Load took: ")
def load(docs,start_date):
    conn = postprocessing_connection()
    db = conn.drivers
    last_month_collection = db[LATEST_MONTH_COLLECTION]
    monthly_collection = db[MONTHLY_COLLECTION]
    try:
        print("Starting to load last month's collection.")
        last_month_collection.drop() # easiest way to refill the collection
        last_month_collection.insert_many(docs)
        print("Creating index for last month's collection.")
        last_month_collection.create_index([ ("d", 1) ])
        print("Inserting docs into the monthly collection.")
        monthly_collection.insert_many(docs)
        print("Aggregating monthly trends")
        monthly_trends_query = aggregate_monthly_trends(start_date)
        print(monthly_trends_query)
        monthly_collection.aggregate(monthly_trends_query,\
        maxTimeMS=1000000,allowDiskUse=True)
    except Exception as e:
        print(e)
        raise
    finally:
        conn.close()

"""
ETL flow control logic. The ETL is intended for 1 calendar month of data.
Default start_date, end_date are always the 1st day last calendar month's and
the 1st day of this calendar month.
"""
def etl(start_date,end_date):
    #extract
    print("Start extracting...")
    docs = extract(start_date,end_date)
    print("start parsing:")
    if len(docs) > 0:
        #transform
        monthly_list = transform_list(docs)
        #check
        check_if_data_exists_for_range_of_dates(start_date,end_date)
        #load
        print("Start inserting into Mongo:")
        load(monthly_list,start_date)
        print("inserted {0} docs".format(len(monthly_list)))
    else:
        logging.info("no docs. Check the query")
    print("Done.")

"""
Execution of the script (Parses arguments and controls flow of the script).
Flow of the execution:
If --no-default_dates flag is set, can set up custom -start_date and -end_date,
but make sure that they are exactly 1 month apart (example: -start_date '2020-04-01'
-end_date '2020-05-01')
"""
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-end_date') # format '%Y-%m-%d'
    parser.add_argument('-start_date')
    parser.add_argument('--no-default_dates', dest='default_dates', action='store_false')
    parser.set_defaults(default_dates=True)
    options = parser.parse_args()
    print('connecting..')
    print('deriving dates...')
    if (options.default_dates is True):
        start_date,end_date = default_start_and_end_date()
    else:
        start_date,end_date = (options.start_date,options.end_date)
    print('start date %s , end date %s' % (start_date,end_date))
    print('starting ETL..')
    etl(start_date,end_date)


if __name__ == "__main__":
    main()
