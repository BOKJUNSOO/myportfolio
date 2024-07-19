import argparse
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

from base import read_input, init_df, df_with_meta
from filter import DailyStatFilter, TopRepoFilter, TopUserFilter, ToplanFilter
from es import Es
import os


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target_date", default=None, help="optional:target date(yyyy-mm-dd)")
    
    '''argument에 1)target_date 2)spark 3)input_path  전달해서 class에서 사용'''
    # args 객체생성
    args = parser.parse_args()
    
    # 1) args + target_date
    '''수집된 데이터의 일자와 Elastic search 의 indexing(@timestamp)'''
    args.target_date = "2024-07-07"     # hyper parameter

    # spark 객체생성
    spark = (SparkSession
        .builder
        .master("local")
        .appName("spark-sql")
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
        .config("spark.jars", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
        .getOrCreate())
    
    '''spark, input_path'''
    # 2) args + spark
    args.spark = spark  

    # 3) args + input_path
    # whole data of wget target_date
    args.input_path = f"/opt/bitnami/spark/data/{args.target_date}-*.json"
    

    # read data
    df = read_input(args.spark, args.input_path)
    df = init_df(df)

    df.filter(df.language.isNotNull()).show(10)

    # base filtering data (without suffling)
    df.coalesce(1).write.format('json').save(f"{args.input_path}\{args.target_date}.json")

    # daily stat filter
    stat_filter = DailyStatFilter(args)
    stat_df = stat_filter.filter(df)
    stat_df = df_with_meta(stat_df, args.target_date)

    # top repo filter
    repo_filter = TopRepoFilter(args)
    repo_df = repo_filter.filter(df)
    repo_df = df_with_meta(repo_df, args.target_date)

    # top user filter
    user_filter = TopUserFilter(args)
    user_df = user_filter.filter(df)
    user_df = df_with_meta(user_df, args.target_date)

    # daily language
    lan_filter = ToplanFilter(args)
    lan_df = lan_filter.filter(df)
    lan_df = df_with_meta(lan_df, args.target_date)

    stat_df.show()
    repo_df.show()
    user_df.show()
    lan_df.show()

    # store data to ES
    # http://localhost:5601/app/home#/
    # ES 환경변수 설정..!
    es = Es("http://es:9200")
    es.write_df(stat_df, "daily-stats-2024")
    es.write_df(repo_df, "top-repo-2024")
    es.write_df(user_df, "top-user-2024")
    es.write_df(lan_df, "daily-language-2024")
