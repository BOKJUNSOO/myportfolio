# datapipline
datapipline with pyspark

## Directory Structure

```
| data
  |- data with github archive
| jobs
  |- pyspark.py and shell.sh
| resources
  |- .jars for spark third-party app(elastic Search)
| docker-compose.yml
```

## How to run pyspark project

run containers:

``` bash
$ docker-compose up -d
```

spark-submit:

``` bash
$ docker exec -it datapipline-spark-master-1 spark-submit --jars <resource/jarsfile.jar> --master spark://spark-master:7077 jobs/main.py data/<filename>
```