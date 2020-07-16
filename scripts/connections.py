import pdb
from pyathenajdbc import connect
import pymongo
import os

S3_STAGING_DIR = 's3://aws-athena-query-results-348517286947-us-east-1/'
S3_REGION_NAME = 'us-east-1'


def prod_connection_string(username,password):
    return "mongodb://{}:{}@datawarehouseprod-shard-00-00-coq6x.mongodb.net:27017,datawarehouseprod-shard-00-01-coq6x.mongodb.net:27017,datawarehouseprod-shard-00-02-coq6x.mongodb.net:27017/test?ssl=true&replicaSet=DataWarehouseProd-shard-0&authSource=admin".format(username,password)

def postprocessing_connection_string(username,password):
    # When it's time to run for real, export NATALYA_CLUSTER_URI="mongodb+srv://{}:{}@cluster0-ee68b.mongodb.net/test"
    uri = os.environ.get('NATALYA_CLUSTER_URI', 'localhost:27017').format(username,password)
    return uri



def get_postprocessing_secrets():
    u_postprocessing = os.environ.get('U_POSTPROCESSING')
    pw_postprocessing = os.environ.get('PW_POSTPROCESSING')
    return (u_postprocessing, pw_postprocessing)

def get_athena_secrets():
    access_key = os.environ.get('ATHENA_ACCESS_KEY')
    secret_key = os.environ.get('ATHENA_ACCESS_SECRET')
    return (access_key, secret_key)

def postprocessing_connection():
    user,password = get_postprocessing_secrets()
    my_cluster = pymongo.MongoClient(\
        postprocessing_connection_string(user,password)\
        ,retryWrites = True,socketTimeoutMS = 3600000)
    return my_cluster

def athena_connection():
    access_key, secret_key = get_athena_secrets()
    conn = connect(access_key=access_key,
               secret_key=secret_key,
               s3_staging_dir=S3_STAGING_DIR,
               region_name=S3_REGION_NAME)
    return conn
