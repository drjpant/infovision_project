import os
import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, StorageLevel
from pyspark import SparkConf
from sys import stdin

from pyspark.sql.functions import current_date, current_timestamp

#from pyspark.sql.connect.functions import current_date

if os.environ.get('Foo') is not None:
    print('something in environment')

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
print('spark context set')


my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")


spark= SparkSession.builder.config(conf= my_conf).getOrCreate()

DF= spark.read.option("header",True).option("inferSchema",True).json(r"C:\Users\91812\Downloads\data1.txt")
DF=DF.drop("cluster_id","authors_byline",)
DF=DF.withColumn("insertdate",current_date()).withColumn("insertdtm",current_timestamp())
#orderDF=orderDF.withColumn("insertdtm",current_timestamp())
#groupdf=orderDF.repartition(4).where("order_customer_id> 10000").select("order_id","order_customer_id") \
#    .groupby("order_customer_id").count()
DF.show(20)

DF.write.parquet(r"C:\Users\91812\Downloads\data1.parquet")
#orderDF.printSchema()

#orderDF.show()
spark.stop()




