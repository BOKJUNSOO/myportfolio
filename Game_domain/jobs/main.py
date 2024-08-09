import argparse
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

from Game_domain.jobs.data_pipline.base import read_input, read_input2, status_df ,init_df, init2_df
from Game_domain.jobs.data_pipline.filter import TopClassFilter, TopExpClassFilter ,TopExpUserFilter, Congratulation
# four data model that i wanted
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

    # yesterday ranking data
    args.target_date2 = (datetime.now() - timedelta(1)).strftime("%y-%m-%d")
    args.input_path = f"/opt/bitnami/spark/data/ranking_{args.target_date2}.json"

    df_t = read_input(args.spark, args.input_path)
    df_y = read_input2(args.spark , args.input_path2)
    
    # load preprocessed data
    df = init_df(df_t)

    #1) daily status filter 
    dist_filter = TopClassFilter(args)
    dist_df = status_df(df)
    dist_df = dist_filter.filter(dist_df)

    # load preporcessed data
    df_t = init_df(df_t)
    df_y = init_df(df_y)
    df2 = init2_df(df_t, df_y)

    #2) top exp user filter (with df2)
    exp_user = TopExpUserFilter(args)
    expuser_df = exp_user.filter(df2)

    #3) top exp class filter (with df2)
    exp_class = TopExpClassFilter(args)
    expclass_df = exp_class.filter(df2)

    #4) congratulation filter
    con_user = Congratulation(args)
    congra_df = con_user.filter()

    dist_df.show(10, False)
    expuser_df.show(10,False)
    expclass_df.show(10,False)
    