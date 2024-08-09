from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder
    .master("local")
    .appName("exp_info")
    .getOrCreate()
)

file_path = "/opt/bitnami/spark/data/ranking_2024-08-09.json"

df = spark.read \
            .format("json") \
            .option("multiLine",True) \
            .option("header", True) \
            .option("inferschema", True) \
            .load(file_path)

file_path = "/opt/bitnami/spark/data/ranking_2024-08-08.json"
df_b = spark.read \
            .format("json") \
            .option("multiLine",True) \
            .option("header", True) \
            .option("inferschema", True) \
            .load(file_path)

# select column to use
df1 = df.select(F.explode("ranking")
                     .alias("ranking_info"))

df1 = df1.select("ranking_info.character_name",
                 "ranking_info.character_level",
                 "ranking_info.character_exp",
                 "ranking_info.class_name",
                 "ranking_info.sub_class_name")

df2 = df_b.select(F.explode("ranking")
                     .alias("ranking_info"))

df2 = df2.select("ranking_info.character_name",
                 "ranking_info.character_level",
                 "ranking_info.character_exp",
                 "ranking_info.class_name",
                 "ranking_info.sub_class_name")

# fill out the empty class
df1 = df1.withColumn("class" , df1["sub_class_name"])
df1 = df1.withColumn("class" ,
                     F.when(F.col("sub_class_name") == "" , df1["class_name"]) \
                      .otherwise(F.col("class")))
df1 = df1.drop("class_name", "sub_class_name")

df2 = df2.withColumn("class" , df2["sub_class_name"])
df2 = df2.withColumn("class" ,
                     F.when(F.col("sub_class_name") == "" , df2["class_name"]) \
                      .otherwise(F.col("class")))
df2 = df2.drop("class_name", "sub_class_name")

# inner join with two dataframe on character_name

df_j = df1.join(df2, 
                (df1["character_name"] == df2["character_name"]) &
                (df1["character_level"] == df2["character_level"]),
                "inner") 

df_j = df_j.withColumn("increase_exp", (df1["character_exp"] - df2["character_exp"]))

df_j = df_j.select(df1["character_name"],
                   df_j["increase_exp"],
                   df1["character_level"],
                   df1["class"]
                  ) \
            .orderBy(F.desc("increase_exp"))
df_j.show(10,False)
