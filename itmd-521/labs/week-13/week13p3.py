from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', os.getenv('ACCESSKEY'))
conf.set('spark.hadoop.fs.s3a.secret.key', os.getenv('SECRETKEY'))
conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set('spark.hadoop.fs.s3a.committer.magic.enabled', 'true')
conf.set('spark.hadoop.fs.s3a.committer.name', 'magic')
conf.set("spark.hadoop.fs.s3a.endpoint", "http://infra-minio-proxy-vm0.service.consul")

# Setting the legacy time parser policy to handle date parsing issues
# conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

spark = SparkSession.builder.appName("vvittanala convert 90.text to csv part 3").config('spark.driver.host', 'spark-edge.service.consul').config(conf=conf).getOrCreate()

splitDF = spark.read.parquet('s3a://vvittanala/90.parquet')


temp_df = splitDF.filter((splitDF["AirTemperature"] >= -40) & (splitDF["AirTemperature"] <= 50))
# created a temp_df variable to filter temperature values in human survival values

ymfilter_df = temp_df.select(
    year("ObservationDate").alias("year"),
    month("ObservationDate").alias("month"),
    "AirTemperature"
)
# selected a columns as ObservationDate, AirTemperture and extracted it into a year & month colum

avg_df = ymfilter_df.groupBy("year", "month").agg(avg("AirTemperature").alias("avg_temperature"))
# found average temperature of Airtemperature by using Aggregate function as avg
stdev_df = avg_df.select("month","avg_temperature").groupBy("month").agg(stddev("avg_temperature").alias("stddev_temperature")).orderBy("month")
# founded standard deviation of average temperature by using Aggregate function as stddev

stdev_df.write.format("parquet").mode("overwrite").option("header", "true").save("s3a://vvittanala/part-three.parquet")
# written stdev_df as aparquet file in s3a bucket

new_df = stdev_df
new_df.write.format("csv").mode("overwrite").option("header","true").save("s3a://vvittanala/part-three.csv")
# written new_df as a csv file in s3a bucket




spark.stop()