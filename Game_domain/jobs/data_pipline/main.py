import argparse
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

from base import read_input, init_df, init2_df
from filter import TopClassFilter, TopExpClassFilter ,TopExpUserFilter

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    spark = (SparkSession
             .builder
             .master("local")
             .appName("spark-nexon_api")
             .config("spark.jars", "/opt/bitnami/spark/resources/mysql-connector-j-9.0.0.jar")
             .getOrCreate())
    args.spark = spark

    # today ranking data
    args.target_date = datetime.now().strftime("%y-%m-%d")
    args.input_path = f"/opt/bitnami/spark/data/ranking_{args.target_date}.json"

    # last day ranking data
    args.target_date2 = (datetime.now() - timedelta(1)).strftime("%y-%m-%d")
    args.input_path = f"/opt/bitnami/spark/data/ranking_{args.target_date2}.json"

    df = read_input(args.spark, args.input_path)
    df_b = read_input(args.spark , args.input_path2)
    
    # load preprocessed data
    df = init_df(df)

    # daily distribute filter
    dist_filter = TopClassFilter(args)
    dist_df = dist_filter.filter(df)

    # load preporcessed data
    df2 = init2_df(df, df_b)

    # top exp user filter (with df2)
    exp_user = TopExpUserFilter(args)
    expuser_df = exp_user.filter(df2)

    # top exp class filter (with df2)
    exp_class = TopExpClassFilter(args)
    expclass_df = exp_class.filter(df2)
    