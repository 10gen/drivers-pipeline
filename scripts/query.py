import re
from datetime import datetime, timezone
from dateutil.relativedelta import *

SOURCE_TABLE = 'cloud_backend_raw.dw__cloud_backend__rawclientmetadata'
MONTHLY_TRENDS_TABLE = 'drivers_monthly_trends'
def java_driver_names():
    return [
        'mongo-java-driver',
        'mongo-java-driver|mongo-java-driver-reactivestreams',
        'mongo-java-driver|mongo-java-driver-rx',
        'mongo-java-driver|mongo-scala-driver',
        'mongo-java-driver|sync',
        'mongo-java-driver|legacy',
        'mongo-java-driver|async',
        'mongo-java-driver|async|mongo-java-driver-reactivestreams',
        'mongo-java-driver|async|mongo-scala-driver'
    ]

def parse_java(platform_name,driver_name):
    if driver_name == 'mongo-java-driver|mongo-scala-driver':
        #'Java/Oracle Corporation/1.8.0_202-b08|Scala/2.12.6'
        split_str = platform_name.replace('|','/').split('/')
        language_version = { 'java': split_str[2], 'scala': split_str[4]}
    else:
        #Java/Oracle Corporation/1.8.0_181-b15
        language_version = platform_name.split('/')[2]
    provider = platform_name.split('/')[1]
    return (provider,language_version)

def python_driver_names():
    return [
        'PyMongo',
        'PyMongo|Motor',
        'PyMongo|PyMODM',
    ]

def parse_python(platform_name):
    split_str = re.split(r'\|',platform_name)
    language_version = re.sub(r'\.(final|candidate)\.\d','',split_str[0]).replace(' ','')
    framework = (None if (len(split_str) == 1) else split_str[1].replace(' ','')) #TODO: tornado version?
    return (language_version,framework)

def node_driver_names():
    return [
        'nodejs',
        'nodejs-core'
        ]

def parse_node(platform_name):
    #'Node.js v8.11.3, LE, mongodb-core: 3.2.5'
    split_str = platform_name.replace('Node.js v','').split(',')
    language_version = split_str[0]
    return language_version

def ruby_driver_names():
    return [
        'mongo-ruby-driver'
    ]

def parse_ruby(platform_name):
    #'mongoid-6.3.0, 2.5.1, x86_64-linux-musl, x86_64-pc-linux-musl',
    # '2.4.6, x86_64-linux, x86_64-pc-linux-gnu'
    framework, language_version = (None,None)
    split_str = platform_name.split(',')
    if platform_name.find('mongoid') > -1:
        framework = split_str[0]
        language_version = split_str[1]
    else:
        language_version = split_str[0]
    return (framework, language_version)

def go_driver_names():
    return ['mongo-go-driver']

def parse_go(platform_name):
    language_version = platform_name.replace('go','')
    return language_version

def csharp_driver_names():
    return ['mongo-csharp-driver']

def parse_csharp(platform_name):
    #Mono 5.14.0 (explicit/969357ac02b)
    #.NET Core 4.6.26926.01
    #NET Framework 4.6.1586.0
    pattern = r'.?([a-zA-Z]+ )+' # find 1 or more words that do not contain numbers
    framework = {
        'fname': re.search(pattern,platform_name).group(0).rstrip(),
        'fv': re.sub(pattern,'',platform_name).split(' ')[0]
    }
    return framework

def  perl_driver_names():
    return ['MongoDB Perl Driver']

def parse_perl(platform_name):
    language_version = platform_name.split(' ')[1].replace('v','')
    return language_version

def other_drivers_names():
    return [
                'mgo',
                'mongo-rust-driver-prototype',
                'mongo-rust-driver',
                'mongoc',
                'mongoc / ext-mongodb:HHVM',
                'mongoc / ext-mongodb:PHP',
                'mongoc / mongocxx',
                'mongoc / MongoSwift',
                'MongoKitten',
                'nodejs|Mongoose',
                'mongo-java-driver|mongo-spark',
                'mongo-java-driver|legacy|mongo-spark',
                'mongo-java-driver|sync|mongo-kafka',
                'mongo-java-driver|sync|mongo-kafka|sink',
                'mongo-java-driver|sync|mongo-kafka|source',
            ]

def driver_names():
    return (java_driver_names() + python_driver_names() + \
    node_driver_names() + ruby_driver_names() + go_driver_names() + \
    csharp_driver_names() + perl_driver_names() + other_drivers_names())



def driver_names_list():
    return "'" + "','".join(driver_names())+"'"

"""
Constructs the query for monthly drivers usage aggregation based on start and
end date
"""
def query_drivers(start_date,end_date):
    query = "with prep as (SELECT rt AS ts,\
            entries__raw__driver__name AS d,\
            entries__raw__driver__version AS dv,\
            gid__oid as gid,\
            entries__raw__os__name AS os,\
            entries__raw__os__architecture AS osa,\
            entries__raw__os__version AS osv,\
            entries__raw__platform AS p,\
            mv AS sv,\
            day_of_month(rt) AS day,\
            month(rt) AS m,\
            year(rt) AS y\
       FROM {0}\
       where\
       entries__raw__driver__name IN ({1})\
       and processed_date >= '{2}' and processed_date <= '{3}'\
       and rt >= date '{2}' and rt < date '{3}' and \
       (entries__raw__application__name is NULL or \
       (entries__raw__application__name not in('mongodump','mongotop','mongorestore','mongodrdl','mongosqld',\
                                            'mongoimport','mongoexport','mongodump','mongorestore',\
                                             'mongomirror','mongofiles') and \
    entries__raw__application__name not like 'stitch|%' and \
    lower(entries__raw__application__name) not like 'mongodb%' and \
          entries__raw__application__name not like 'mongosh%' and \
    entries__raw__application__name not like 'realm|%')))\
    SELECT  d,\
            dv,\
            gid,\
            os,\
            osa,\
            osv,\
            p,\
            sv,\
            m,\
            y,\
            max(ts) as ts \
    FROM prep GROUP BY  d, dv, gid, os, osa, osv, p, sv, m, y".\
        format(SOURCE_TABLE,driver_names_list(),start_date,end_date)
    return query

def aggregate_monthly_trends(start_date):
    start_date = datetime.strptime(start_date,'%Y-%m-%d')
    trends_start_date = start_date + relativedelta(months=-6)
    return [
    {
        '$match': {
            'd': {
                '$ne': 'mongo-go-driver'
            },
            'ts': {
                '$gte': trends_start_date
            }
        }
    }, {
        '$project': {
            'd': {
                '$switch': {
                    'branches': [
                        {
                            'case': {
                                '$gt': [
                                    {
                                        '$indexOfCP': [
                                            '$d', 'kafka'
                                        ]
                                    }, -1
                                ]
                            },
                            'then': 'kafka'
                        }, {
                            'case': {
                                '$gt': [
                                    {
                                        '$indexOfCP': [
                                            '$d', 'spark'
                                        ]
                                    }, -1
                                ]
                            },
                            'then': 'spark'
                        }, {
                            'case': {
                                '$gt': [
                                    {
                                        '$indexOfCP': [
                                            '$d', 'java'
                                        ]
                                    }, -1
                                ]
                            },
                            'then': 'java'
                        }, {
                            'case': {
                                '$gt': [
                                    {
                                        '$indexOfCP': [
                                            '$d', 'nodejs|Mongoose'
                                        ]
                                    }, -1
                                ]
                            },
                            'then': 'nodejs | Mongoose'
                        }, {
                            'case': {
                                '$gt': [
                                    {
                                        '$indexOfCP': [
                                            '$d', 'nodejs'
                                        ]
                                    }, -1
                                ]
                            },
                            'then': 'nodejs'
                        }
                    ],
                    'default': '$d'
                }
            },
            'm': 1,
            'y': 1,
            'sv': 1,
            'dv': 1,
            'lver': 1,
            'gid': 1,
            'ts': 1,
            'i': 1
        }
    },
    {
    '$out': MONTHLY_TRENDS_TABLE
    }
]
