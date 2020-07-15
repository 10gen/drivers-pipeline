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
from queries_tools import collections_queries
from connections import athena_connection, postprocessing_connection

"""
Method for running an Athena query and returning result as list of dicts.
"""
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

"""
Load step of the ETL for one collection and one list of docs. Creates a new
collection, loads data, then in case of success deletes the old one and
renames the new one (i.e. removes the suffix "_new")
"""
def load(collection,docs):
    conn = postprocessing_connection()
    db = conn.dev_tools
    output_collection = db[collection]
    try:
        print("Filling up table {}".format(collection))
        #pdb.set_trace()
        output_collection.insert_many(docs)
        print('inserted docs')
    except Exception as e:
        print(e)
        raise
    finally:
        conn.close()

"""
ETL for one collection
"""
def etl_for_one_collection(collection,query):
    #extract
    docs = run_query(query)
    if len(docs) > 0:
        #load
        load(collection,docs)
    else:
        print('no results from the query')

"""
Running ETL for list or queries/collections
"""
def etl_for_list_of_queries():
    print("starting Athena ETLs")
    for doc in collections_queries():
        query = list(doc.values())[0]
        collection_name = list(doc.keys())[0] # creating new collection to be later removed and replaced.
        etl_for_one_collection(collection_name,query)

if __name__ == "__main__":
    etl_for_list_of_queries()
