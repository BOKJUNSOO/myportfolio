from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import when
import sys
from datetime import *

#
spark = (
    SparkSession.builder
    .appName("Rank_info")
    .master("local")
    .getOrCreate()
)

# load
target_date = "2024-08"
file_path = f"/opt/bitnami/spark/data/ranking_{target_date}-*.json"
df = spark.read.format("json") \
               .option("multiLine",True) \
               .option("header", True) \
               .option("inferschema", True) \
               .load(file_path)
df.printSchema()

# select column to use

df_flat = df.select(F.explode("ranking")
                     .alias("ranking_info"))

df_flat = df_flat.select("ranking_info.date",
                         "ranking_info.character_level",
                         "ranking_info.class_name",
                         "ranking_info.sub_class_name")

# fill class

df_flat = df_flat.withColumn("class" , df_flat["sub_class_name"])
df_flat = df_flat.withColumn("class",
                             F.when(F.col("sub_class_name") == "", df_flat["class_name"])\
                              .otherwise(F.col("class")))
df_flat = df_flat.drop("class_name","sub_class_name")


# Categorization level range
df_flat = df_flat.withColumn("status" ,
                             when(df_flat["character_level"] > 289, "Tallahart")
                             .when((df_flat["character_level"] < 290) &(df_flat["character_level"] > 284) , "Carcion")
                             .when((df_flat["character_level"] < 285) & (df_flat["character_level"] > 279) , "Arteria")
                             .when((df_flat["character_level"] < 280) & (df_flat["character_level"] > 274) , "Dowonkyung")
                             .otherwise(F.col("character_level")))

df_flat.show(10,False)

# groupBy level range
df_group = df_flat.groupBy("class","date") \
                  .pivot("status").count()

# sum Carcion + Arteria + Dowonkyung
map_list = ["Arteria","Carcion","Dowonkyung","Tallahart"]

df_group = df_group.withColumn("sum",
                               sum([F.col(c) for c in map_list]))

# set a key value
df_group = df_group.withColumn("key_value",
                               (F.concat(
                                   F.col("date"),
                                   F.lit("-"),
                                   F.col("class")
                                   )
                               ))
df = df_group.select(["key_value","sum",
                      "Tallahart","Carcion","Arteria","Dowonkyung"])
df = df.orderBy(F.desc("Tallahart"))
df.show(10, False)

# join data that already filtering


