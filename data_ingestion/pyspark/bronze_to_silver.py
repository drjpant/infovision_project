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
df1=spark.read.option("header",True).option("inferSchema",True).parquet(r"C:\Users\91812\Downloads\data1.txt")
DF2=DF.union(df1)
#DF=DF.withColumn("insertdate",current_date()).withColumn("insertdtm",current_timestamp())
#orderDF=orderDF.withColumn("insertdtm",current_timestamp())
#groupdf=orderDF.repartition(4).where("order_customer_id> 10000").select("order_id","order_customer_id") \
#    .groupby("order_customer_id").count()
DF2.show(20)

DF2.write.parquet(r"C:\Users\91812\Downloads\data1.parquet").mode("overwrite")
#orderDF.printSchema()

#orderDF.show()
spark.stop()




