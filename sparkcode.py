from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql.types import *

spark=SparkSession.builder.appName("spark").getOrCreate()

df=spark.read.format("csv").option("header","true").load("path")
#This is the action command
df.show()
df.printSchema()