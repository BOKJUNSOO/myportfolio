import glob

from abc import ABC, abstractmethod
from pyspark.sql.types import StructType, StructField, StringType, LongType

import pyspark.sql.functions as F

actor_schema = StructType([
    StructField('login', StringType(), True),
    StructField('url', StringType(), True)
])

payload_schema = StructType([
    StructField('repository_id', LongType(), True),
    StructField('size', LongType(), True),
    StructField('distinct_size', LongType(), True),
    StructField('comment', StructType([StructField('body', StringType(), True)]), True),
])

repo_schema = StructType([
    StructField('name', StringType(), True),
    StructField('url', StringType(), True)
])

def read_input(spark, input_path):
    def _input_exists(input_path):
        return glob.glob(input_path)

    if _input_exists(input_path):
        df = spark.read.json(input_path)
        df.printSchema()
        # max_executor_num = 3
        # if df.rdd.getNumPartitions() < max_executor_num:
        #     df = df.repartition(max_executor_num)
        return df
    else:
        return print("데이터경로 혹은 수집된 데이터의 일자를 확인해주세요.")
    
def init_df(df):
    df = df.select('created_at', 'id', 'payload', 'type',       
                    df.actor.login.alias('login'),
                    df.actor.url.alias('url'), 
                    'repo')
    
    df = df.select('login', 'url', 'created_at', 'id',
                   df.payload.repository_id.alias('repository_id'), \
                   df.payload.size.alias('size'), 
                   df.payload.distinct_size.alias('distinct_size'), \
                   df.payload.pull_request.base.repo.language.alias('language'), \
                   df.payload.comment.alias('comment'),
                   'type', 'repo')
    
    df = df.select('login', 'url', 'created_at', 'id',
                   'repository_id', 'size', 'distinct_size','language', 'comment', 
                   'type', \
                   F.col('repo.name').alias('name'), df.repo.url.alias('repo_url'))
    
    n_cols = ['user_name', 'url', 'created_at', 'id', 'repository_id', 'size', 'distinct_size','language', 'comment', \
            'type', 'name', 'repo_url']
    
    df = df.toDF(*n_cols)

    df = df.filter(F.col("login") != "github-actions[bot]")
    df = df.withColumn('created_at', F.trim(F.regexp_replace(df.created_at, "[TZ]", " ")))
    df = df.withColumn('created_at', F.to_timestamp(df.created_at, 'yyyy-MM-dd HH:mm:ss'))

    udf_check_repo_name = F.udf(lambda name: name.split("/")[1], StringType())
    df = df.withColumn('repo_name', udf_check_repo_name(F.col('name')))
    df = df.drop('name')
    return df

def df_with_meta(df, datetime):
    df = df.withColumn("@timestamp", F.lit(datetime)) #elastic search indexing
    return df


class BaseFilter(ABC):
    def __init__(self, args):
        self.args = args
        self.spark = args.spark     # spark도 args를 가지고 있도록 지정한다.

    def filter(self, df):           # method overdriving
        None
