from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# spark object
spark = (
    SparkSession.builder
    .appName("RDD-test")
    .master("local")
    .getOrCreate()
)

sc = spark.sparkContext

# test sparksession
df = spark.read.json("/opt/bitnami/spark/data/filtering-070718.json")
df.printSchema()
df.filter(df.language.isNotNull()).show(10) # return is none type
df.groupBy('language').count().sort(F.desc("count")).drop('NULL').show(20)

# Dataframe to RDD (read with textFile)
rdd1 = sc.textFile("/opt/bitnami/spark/data/filtering-070718.json")
rdd2 = sc.textFile("/opt/bitnami/spark/data/2024-07-07-18.json")

# RDD actions
print(
    rdd1.count(),   # 134870
    rdd1.countApproxDistinct(),     # 138926
    sep = "/"
    )

print(
    rdd2.count(),   #180328
    rdd2.countApproxDistinct(), #185243
    sep = '/'
    )
# stop sparksession
spark.stop()