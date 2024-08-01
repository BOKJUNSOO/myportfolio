from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import *

#
spark = (
    SparkSession.builder
    .appName("Spark-with-Nexon")
    .master("local")
    .getOrCreate()
)

# init_ and load
file_path = sys.argv[1]
df = spark.read.format("json") \
               .option("multiLine",True) \
               .option("header", True) \
               .option("inferschema", True) \
               .load(file_path)

df.printSchema()

df_flat = df.select(F.explode("potential_history")
                     .alias("potential_info"))
df_flat = df_flat.select("potential_info.after_potential_option.value"
                         ,"potential_info.after_potential_option.grade")

df_flat.show(10, False)




# filter
# F.explode
"""
distinct_count_df = df.groupBy("value") \
                      .count() \
                      .alias("value_count")
distinct_count_df = distinct_count_df.sort(F.desc("count")) \
                                     .limit(10)
#distinct_count_df.show(10, False)"""

