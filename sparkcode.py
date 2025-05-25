from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql.types import *

from pyspark.sq import Window

spark=SparkSession.builder.appName("spark").getOrCreate()

df=spark.read.format("csv").option("header","true").load("path")

df.show()
df.printSchema()