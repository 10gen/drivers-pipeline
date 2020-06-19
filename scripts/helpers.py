from datetime import datetime
import pdb

"""
Finds max date of the data in a mongo collection
"""
def max_available_date(collection):
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
    result = run_aggregation(collection,pipeline)
    if not result:
        last_date = None
    else:
        last_date = result[0]['max']
    return  last_date

"""
Runs query that deletes all documents in a mongo collection between start and
end date
"""
def query_delete_many(start_date,end_date,collection):
    query = {
        'ts': {
            '$gte': datetime.strptime(start_date, "%Y-%m-%d"),
            '$lt': datetime.strptime(end_date, "%Y-%m-%d"),
        }
    }
    print(query)
    result = collection.delete_many(query)
    return result

def group_external_apps(list):
    df = pd.DataFrame(list)
    if 'a' in df.columns:
        df = df.drop(columns=['a'])
    df = df.replace('', ' ', regex = True)
    df = df.replace(np.nan, '', regex = True)
    df = df.groupby(['d','p','month','day','year','sv','dv','os','osa','osv','gid'],as_index=False).max().drop_duplicates()
    df = df.replace({'': None})
    df = df.replace(' ', '')
    d_dict = df.to_dict('r')
    return d_dict

def run_aggregation(collection,pipeline,maxMS=10000000,allowDisk=True):
    return list(collection.aggregate(pipeline,maxTimeMS = maxMS,allowDiskUse=allowDisk))

"""
Decorator that wraps a method into calculation of time elapsed
message - text to display next to time elapsed
"""
def function_with_time_elapsed(message):
    def decorator(function):
        def wrapper(*args,**kwargs):
            start_time = datetime.today()
            res = function(*args,**kwargs)
            end_time = datetime.today()
            time_elapsed = end_time - start_time
            print("{}_{}".format(message,time_elapsed))
            return res
        return wrapper
    return decorator

#TODO: in case need to create index every time.
#def create_indexes():
    #db.drivers_stats.dropIndex("app_name_text"); db.drivers_stats.createIndex({a: "text", ts: 1},{name: "app_ts_text_compound"});
