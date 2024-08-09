from Game_domain.jobs.data_pipline.base import BaseFilter
import pyspark.sql.functions as F
from pyspark.sql import Window


# daily filter
class TopClassFilter(BaseFilter):
    def filter(self, df):
        class_df = df.groupBy("class","date") \
                      .pivot("status").count()
        
        map_list = ["Tallahart", "Carcion", "Arteria", "Dowonkyung"]
        class_df = class_df.withColumn("sum"
                                         ,sum(F.col(c) for c in map_list))

        window = Window.orderBy(F.desc("sum"))
        class_df = class_df.withColumn("rank", F.rank().over(window))

        class_df = class_df.select(["class",
                                    "Tallahart","Carcion","Arteria","Dowonkyung",
                                    "sum","rank","date"])
        class_df.show(10,False)
        
        return class_df

# detect exp history (compare with yesterday data)
# depend on init2_df method
class TopExpUserFilter(BaseFilter):
    def filter(self, df):
        df = df.select(df["character_name"],
                       df["increase_exp"],
                       df["character_level"],
                       )\
                .orderBy(F.desc("increase_exp"))
        return df
    
class TopExpClassFilter(BaseFilter):
    def filter(self, df):
        return None
#
class Congratulation(BaseFilter):
    def filter(self, df):
        return None
    
