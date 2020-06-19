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
#import logging
import time
from pyathenajdbc import connect
from queries_connectors import *
from connections import athena_connection, postprocessing_connection

def load(collection,docs):
    conn = postprocessing_connection()
    db = conn.connectors
    output_collection_new = db['%s_new' % collection]
    output_collection = db[collection]
    try:
        print("Filling up table {}".format(collection))
        #pdb.set_trace()
        output_collection_new.insert_many(docs)
        print('inserted docs')
    except Exception as e:
        print(e)
        raise
    try:
        print('dropping old collection')
        output_collection.drop()
        print('renaming new collection')
        output_collection_new.rename(collection)
    except Exception as e:
        print(e)
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

def etl_for_list_of_queries():
    print("starting Athena ETLs")
    for doc in collections_queries:
        query = list(doc.values())[0]
        collection_name = list(doc.keys())[0] # creating new collection to be later removed and replaced.
        etl_for_one_collection(collection_name,query)

if __name__ == "__main__":
    etl_for_list_of_queries()
