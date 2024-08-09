import glob

from abc import ABC, abstractmethod
import pyspark.sql.functions as F
from pyspark.sql.functions import when

class BaseFilter(ABC):
    def __init__(self, args):
        self.args = args
        self.spark = args.spark
        
    def filter (self,df):
        None

# Check data exist
def read_input(spark, input_path):
    def _input_exists(input_path):
        return glob.glob(input_path)
    
    if _input_exists(input_path):
        df = spark.read.json(input_path)
        df.printSchema()

        return df
    else:
        return print("파일이 위치하지 않습니다.")

def read_input2(spark, input_path):
    def _input_exists(input_path):
        return glob.glob(input_path)
    
    if _input_exists(input_path):
        df = spark.read.json(input_path)
        df.printSchema()

        return df
    else:
        return print("전날 데이터를 먼저 수집해주세요.")

# Preprocessing for data store
def init_df(df):
    # select column to use
    df = df.select(F.explode("ranking")
                         .alias("ranking_info"))
    df = df.select("ranking_info.date",
                   "ranking_info.character_level",
                   "ranking_info.character_exp",
                   "ranking_info.class_name",
                   "ranking_info.sub_class_name")
    # fill class (at least not null)
    df = df.withColumn("class", df["sub_class_name"])
    df = df.withColumn("class",
                       F.when(F.col("sub_class_name") == "", df["class_name"]) \
                        .otherwise(F.col("class")))
    df = df.drop("class_name", "sub_class_name")
    df.show(10,False)
    return df

# Categorization level range
def status_df(df):

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

# join today and yesterday dataframe (when use, depend on init_df)
def init2_df(df_t, df_y):
    df = df_t.join(df_y,
                    (df_t["character_name"] == df_y["character_name"]) &
                    (df_t["character_level"] == df_y["character_level"]),
                    "inner")
    df = df.withColumn("increase_exp",
                       (df_t["character_exp"] - df_y["character_exp"]))
    return df
