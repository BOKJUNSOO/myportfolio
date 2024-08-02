from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import *

#
spark = (
    SparkSession.builder
    .appName("Cube_potential")
    .master("local")
    .getOrCreate()
)

# load
file_path = sys.argv[1]
df = spark.read.format("json") \
               .option("multiLine", True) \
               .option("header" , True) \
               .option("inferschema" , True) \
               .load(file_path)

df.printSchema()

# init
df_flat = df.select(F.explode("cube_history")   # to object
                     .alias("cube_info"))
df_flat = df.selcet(
    "cube_info.cube_type",
    "cube_info.additional_potential_option_grade",
    "cube_info.after_additional_potential_option"
)


# need config of filter
# sorting cube/ potential / target item

# bin\bash!
# docker exec -it nexonapi-spark-master-1 spark-submit --master spark://spark-master:7077 jobs/Submit_CubeP.py data/myapi_c_2024-05-05.json