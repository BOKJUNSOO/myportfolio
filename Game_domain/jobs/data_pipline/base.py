import glob

from abc import ABC, abstractmethod
import pyspark.sql.functions as F
from pyspark.sql.functions import when

def read_input(spark, input_path):
    def _input_exists(input_path):
        return glob.glob(input_path)
    
    if _input_exists(input_path):
        df = spark.read.json(input_path)
        df.printSchema()

        return df
    else:
        return print("해당 경로에 파일이 위치하지 않습니다.")

def read_input2(spark, input_path):
    def _input_exists(input_path):
        return glob.glob(input_path)
    
    if _input_exists(input_path):
        df = spark.read.json(input_path)
        df.printSchema()

        return df
    else:
        return print("전날 수집된 데이터가 존재하지 않습니다.")

def init_df(df):
    # select column to use
    df = df.select(F.explode("ranking")
                         .alias("ranking_info"))
    df = df.select("ranking_info.date",
                   "ranking_info.character_level",
                   "ranking_info.class_name",
                   "ranking_info.sub_class_name")
    # fill class (at least not null)
    df = df.withColumn("class", df["sub_class_name"])
    df = df.withColumn("class",
                       F.when(F.col("sub_class_name") == "", df["class_name"]) \
                        .otherwise(F.col("class")))
    df = df.drop("class_name", "sub_class_name")
    df.show(10,False)

    # Categorization level range
    df = df.withColumn("status" ,
                       when(df["character_level"] > 289, "Tallahart")
                             .when((df["character_level"] < 290) &(df["character_level"] > 284) , "Carcion")
                             .when((df["character_level"] < 285) & (df["character_level"] > 279) , "Arteria")
                             .when((df["character_level"] < 280) & (df["character_level"] > 274) , "Dowonkyung")
                             .otherwise(F.col("character_level")))
    # set a key value
    df = df.withColumn("key",
                       (F.concat(F.col("date"),
                                 F.lit("-"),
                                 F.col("class")))
                       )
    return df

def init2_df(df, df_b):
    return None


class BaseFilter(ABC):
    def __init__(self, args):
        self.args = args
        self.spark = args.spark
    def filter (self,df):
        None