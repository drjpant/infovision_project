import os
import sys

import args as args
from pyspark.sql import SparkSession
from pyspark import SparkContext, StorageLevel
from pyspark import SparkConf
from sys import stdin
if os.environ.get('Foo') is not None:
    print('something in environment')

#os.environ['PYSPARK_PYTHON'] = sys.executable
#os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
#os.environ["PYSPARK_PYTHON"] = "/Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7"
#os.environ["PYSPARK_DRIVER_PYTHON"] = "/Library/Frameworks/Python.framework/Versions/3.7/bin/python3.7"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
#os.environ['SPARK_HOME']= sys.executable
print('Hello Dheeraj')
print('spark context set')


my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")



spark= SparkSession.builder.config(conf= my_conf).getOrCreate()

#passing the argument
api_file = args['api_file']
secret_name = args['secret_name']

api_token = get_secret_value(sc_client, secret_name=secret_name)

#api_request
url =base_url_siteid+'?keyword_name'=url_encoded_keyword + '&end_date'= end_date)
response = requests.get(url)
get_text_upload_landing_path(response,s3_landing_bucket)




