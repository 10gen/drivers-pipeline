#!/usr/bin/env python

import argparse
import os
import pandas as pd
import pymongo
import dns
from bson.objectid import ObjectId
import datetime
import pprint
import re
import yaml
from datetime import timezone, datetime, tzinfo, date,time, timedelta
import pdb
import logging

logging.basicConfig(filename='drivers_metrics.log',level=logging.INFO)

#root_dir = os.path.abspath(os.path.join(__file__, '../..'))

# todo define Keys, driver names, etc.

MAX_DELTA = 2

def today_midnight():
    today = datetime.today()
    today_midnight = datetime(today.year, today.month, today.day,tzinfo=timezone.utc)
    return today_midnight

def get_date(date_string):
    date_from_string = datetime.strptime(date_string, '%Y%m%d')
    date_midnight = datetime(date_from_string.year,date_from_string.month,date_from_string.day, tzinfo=timezone.utc)
    return date_midnight

def end_date(days=0):
    return today_midnight() - timedelta(days)

def start_and_end_date(end_date_string=None,start_date_string=None):
    end_date = get_date(end_date_string)
    #start_date = end_date -timedelta(delta)
    start_date = get_date(start_date_string)
    return (start_date,end_date)


def default_start_end_date(collection):
    pipeline = [
        {
            '$group': {
                '_id': None,
                'max': {
                    '$max': '$ts'
                }
            }
        }, {
            '$project': {
                'max': 1,
                '_id': 0
            }
        }
    ]
    last_date =  run_aggregation(collection,pipeline)[0]['max']
    last_date_midnight = datetime(last_date.year,last_date.month,last_date.day, tzinfo=timezone.utc)
    print('max found date midnight: %s' % last_date_midnight)
    start_date = last_date_midnight+timedelta(1)
    end_date = today_midnight()
    return  (start_date,end_date)

def driver_names():
    return [
                'mgo',
                'mongo-csharp-driver',
                'mongo-go-driver',
                'mongo-java-driver',
                'mongo-java-driver|mongo-java-driver-reactivestreams',
                'mongo-java-driver|mongo-java-driver-rx',
                'mongo-java-driver|mongo-scala-driver',
                'mongo-ruby-driver',
                'mongo-rust-driver-prototype',
                'mongoc',
                'mongoc / ext-mongodb:HHVM',
                'mongoc / ext-mongodb:PHP',
                'mongoc / mongocxx',
                'mongoc / MongoSwift',
                'MongoDB Perl Driver',
                'MongoKitten',
                'nodejs',
                'nodejs-core',
                'PyMongo',
                'PyMongo|Motor',
                'mongo-java-driver|sync|mongo-kafka'
            ]

def driver_name_condition():
    return list(map(lambda x: {'entries.raw.driver.name': x}, driver_names()))


def pipeline_drivers(start_date,end_date):
    pipeline = [
        {
            '$match': {
                'rt': {
                    '$gte': start_date,
                    '$lt': end_date
                }
            }
        }, {
            '$unwind': {
                'path': '$entries',
                'preserveNullAndEmptyArrays': False
            }
        }, {
            '$match': {
                '$or': driver_name_condition()
            }
        }, {
            '$project': {
                'count': '$entries.count',
                'ts': '$rt',
                'd': '$entries.raw.driver.name',
                'dv': '$entries.raw.driver.version',
                'ts': '$rt',
                'gid': '$gid',
                'os': '$entries.raw.os.name',
                'osa': '$entries.raw.os.architecture',
                'osv': '$entries.raw.os.version',
                'p': '$entries.raw.platform',
                'sv': '$mv',
                'day': {'$dayOfMonth': '$rt'},
                'month': {'$month': '$rt'},
                'year': {'$year': '$rt'}
            }
        },{
            '$group': {
                '_id': {
                    'd': '$d',
                    'dv': '$dv',
                    'gid': '$gid',
                    'os': '$os',
                    'osa': '$osa',
                    'osv': '$osv',
                    'p': '$p',
                    'sv': '$sv',
                    'day': '$day',
                    'month': '$month',
                    'year': '$year'
                },
                'ts': {
                    '$last': '$ts'
                },
                'c': {
                    '$sum': '$count'
                }
            }
        }, {
            '$project': {
                'd': '$_id.d',
                'dv': '$_id.dv',
                'gid': '$_id.gid',
                'os': '$_id.os',
                'osa': '$_id.osa',
                    'p': '$_id.p',
                'sv': '$_id.sv',
                'day': '$_id.day',
                'month': '$_id.month',
                'year': '$_id.year',
                'ts': 1,
                'c': 1,
                '_id': 0
            }
        }
    ]
    return pipeline



def run_aggregation(collection,pipeline,maxMS=10000000,allowDisk=True):
    return list(collection.aggregate(pipeline,maxTimeMS = maxMS,allowDiskUse=allowDisk))


#### TODO: create a class or method for each framework that is parseable. For this framework,
def language_version_and_framework(doc):
    language_version = None
    framework = None
    #print(doc)
    #pdb.set_trace()
    driver_name = doc['d']
    if 'p' in doc:
        try:
            platform_name = doc['p']
            if driver_name in ['PyMongo','PyMongo|Motor']:
                split_str = re.split(r'\|',platform_name)
                language_version = re.sub(r'\.(final|candidate)\.\d','',split_str[0]).replace(' ','') # TODO: PyPy and Python version?
                framework = (None if (len(split_str) == 1) else split_str[1].replace(' ','')) #TODO: tornado version?
            elif driver_name in ['nodejs','nodejs-core']:
                #'Node.js v8.11.3, LE, mongodb-core: 3.2.5'
                split_str = platform_name.replace('Node.js v','').split(',')
                language_version = split_str[0]
            elif driver_name == 'mongo-go-driver':
                #go1.10.8
                language_version = platform_name.replace('go','')
            elif driver_name in ['mongo-java-driver','mongo-java-driver|mongo-java-driver-reactivestreams','mongo-java-driver|mongo-java-driver-rx']:
                #Java/Oracle Corporation/1.8.0_181-b15
                language_version = platform_name.split('/')[2]
            elif driver_name == 'mongo-java-driver|mongo-scala-driver':
                #'Java/Oracle Corporation/1.8.0_202-b08|Scala/2.12.6'
                #TODO: confirm if need versions for both java and scala
                split_str = platform_name.replace('|','/').split('/')
                language_version = { 'java': split_str[2], 'scala': split_str[4]}
            elif driver_name == 'mongo-ruby-driver':
                #'mongoid-6.3.0, 2.5.1, x86_64-linux-musl, x86_64-pc-linux-musl',
                # '2.4.6, x86_64-linux, x86_64-pc-linux-gnu'
                #TODO
                split_str = platform_name.split(',')
                if platform_name.find('mongoid') > -1:
                    framework = split_str[0]
                    language_version = split_str[1]
                else:
                    language_version = split_str[0]
            elif driver_name == 'MongoDB Perl Driver':
                #Perl v5.18.2 x86_64-linux-gnu-thread-multi
                language_version = platform_name.split(' ')[1].replace('v','')
            elif driver_name == "mongo-csharp-driver":
                #Mono 5.14.0 (explicit/969357ac02b)
                #.NET Core 4.6.26926.01
                #NET Framework 4.6.1586.0
                pattern = r'.?([a-zA-Z]+ )+' # find 1 or more words that do not contain numbers
                framework = {
                        'fname': re.search(pattern,platform_name).group(0).rstrip(),
                        'fv': re.sub(pattern,'',platform_name).split(' ')[0]
                }
            else:
                pass

            if language_version is not None:
                doc['lver'] = language_version
            if framework is not None:
                doc['fr'] = framework

        except Exception as e:
            logging.error("Exception %s for doc with platform string %s",e,platform_name)
            print("error detected")
    else:
        logging.info("Document with driver name %s did not have platform field",driver_name)
    return doc

def update_list_with_lang_ver_framework(docs):
    result = list(map(lambda x: language_version_and_framework(x), docs))
    return result

def prod_connection_string(username,password):
    return "mongodb://{}:{}@datawarehouseprod-shard-00-00-coq6x.mongodb.net:27017,datawarehouseprod-shard-00-01-coq6x.mongodb.net:27017,datawarehouseprod-shard-00-02-coq6x.mongodb.net:27017/test?ssl=true&replicaSet=DataWarehouseProd-shard-0&authSource=admin".format(username,password)

def postprocessing_connection_string(username,password):
    return "mongodb+srv://{}:{}@cluster0-ee68b.mongodb.net/test".format(username,password)


def etl(start_date,end_date):
    #pdb.set_trace()
    #extract
    print(pipeline_drivers(start_date,end_date))
    logging.info(pipeline_drivers(start_date,end_date))
    start_time = datetime.today()
    docs = run_aggregation(raw_metadata,pipeline_drivers(start_date,end_date))
    end_time = datetime.today()
    time_elapsed = end_time - start_time
    print("aggregation took {}".format(time_elapsed))
    print("start parsing:")
    start_time = datetime.today()
    if len(docs) > 0:
        #transform
        docs = update_list_with_lang_ver_framework(docs)
        #load
        drivers_test.insert_many(docs)
        logging.info("inserted %s docs",len(docs))
    else:
        logging.info("no docs. Check original dataset")
    end_time = datetime.today()
    time_elapsed = end_time - start_time
    print("parsing took {}".format(time_elapsed))


def full_etl(start_delta,end_date):
    return None

def get_secrets():
    stream = open('secrets.yml', 'r')
    secrets = yaml.load(stream)
    username_dw_prod, pw_dw_prod, u_postprocessing, pw_postprocessing = (secrets['u_dw_prod'],secrets['pw_dw_prod'],secrets['u_postprocessing'],secrets['pw_postprocessing'])
    return (username_dw_prod, pw_dw_prod, u_postprocessing, pw_postprocessing)

def etl_for_range_of_dates(start_date,end_date):
    #start_date = get_date(start_date)
    #end_date = get_date(end_date)
    while (end_date - timedelta(MAX_DELTA)) > start_date:
        interim_start_date = end_date - timedelta(MAX_DELTA)
        print("running etl for start_date: %s, end_date: %s" % (interim_start_date,end_date))
        etl(interim_start_date,end_date)
        end_date = interim_start_date
    if (end_date > start_date):
        print("running etl for start_date: %s, end_date: %s" % (start_date,end_date))
        etl(start_date,end_date)
    print('finished etl')




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    u_dw_prod, pw_dw_prod, u_postprocessing, pw_postprocessing = get_secrets()
    parser.add_argument('-username_dw_prod', default = u_dw_prod)
    parser.add_argument('-pw_dw_prod',default = pw_dw_prod)
    parser.add_argument('-u_postprocessing', default= u_postprocessing)
    parser.add_argument('-pw_postprocessing', default = pw_postprocessing)
    parser.add_argument('-start_delta', default = 7, type=int)
    parser.add_argument('-end_date') # format '%Y%m%d'
    parser.add_argument('-start_date')
    parser.add_argument('--default_dates', dest='since_last', action='store_true')
    parser.add_argument('--no-default_dates', dest='since_last', action='store_false')
    parser.set_defaults(since_last=True)
    options = parser.parse_args()
    #pdb.set_trace()
    print('connecting..')
    prod = pymongo.MongoClient(prod_connection_string(options.username_dw_prod,options.pw_dw_prod))
    dw_raw = prod.dw_raw
    raw_metadata = prod.dw_raw['cloud__cloud_backend__rawclientmetadata']
    my_cluster = pymongo.MongoClient(postprocessing_connection_string(options.u_postprocessing,options.pw_postprocessing))
    db = my_cluster.transactions_metrics
    drivers_test = db['drivers_test_new']
    print('deriving dates...')
    if (options.since_last is True):
        start_date,end_date = default_start_end_date(drivers_test)
    else:
        start_date,end_date = start_and_end_date(options.end_date,options.start_date)
    print('start date %s , end date %s' % (start_date,end_date))
    print('starting ETL..')
    etl_for_range_of_dates(start_date,end_date)
