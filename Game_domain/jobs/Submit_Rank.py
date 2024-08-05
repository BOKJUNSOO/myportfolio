from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import when
import sys

#
spark = (
    SparkSession.builder
    .appName("Rank_info")
    .master("local")
    .getOrCreate()
)

# load
file_path = sys.argv[1]
df = spark.read.format("json") \
               .option("multiLine",True) \
               .option("header", True) \
               .option("inferschema", True) \
               .load(file_path)
df.printSchema()

# select column to use

df_flat = df.select(F.explode("ranking")
                     .alias("ranking_info"))

df_flat = df_flat.select("ranking_info.character_level",
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
df_group = df_flat.groupBy("class") \
                  .pivot("status").count()
df_group = df_group.orderBy(F.desc("Tallahart"))
df_group.show(10, False)


