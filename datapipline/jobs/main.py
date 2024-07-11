import argparse
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

from base import read_input, init_df, df_with_meta
from filter import DailyStatFilter, TopRepoFilter, TopUserFilter, ToplanFilter
from es import Es


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target_date", default=None, help="optional:target date(yyyy-mm-dd)")
    
    '''argument에 spark ,input ,targetdate 전달해서 class에서 사용'''
    #args 객체생성
    args = parser.parse_args()

    # spark 객체생성
    spark = (SparkSession
        .builder
        .master("local")
        .appName("spark-sql")
        .config("spark.driver.extraClassPath", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
        .config("spark.jars", "opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
        .getOrCreate())
    
    '''spark, target_date, input_path'''
    args.spark = spark  
    # if args.target_date is None: 
    #    args.target_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    args.input_path = "/opt/bitnami/spark/data/*.json"      # args.input_path = f"/opt/bitnami/spark/data{args.target_data}*.json"

    df = read_input(args.spark, args.input_path)
    df = init_df(df)

    df.filter(df.language.isNotNull()).show(10)

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
    lan_df = df_with_meta(lan_df, args.targe_date)

    stat_df.show()
    repo_df.show()
    user_df.show()
    lan_df.show()

    # store data to ES
    es = Es("http://es:9200")
    es.write_df(stat_df, "daily-stats-2024")
    es.write_df(repo_df, "top-repo-2024")
    es.write_df(user_df, "top-user-2024")
