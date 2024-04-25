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

df = spark.read.csv('s3a://itmd521/90.txt')

splitDF = df.withColumn('WeatherStation', df['_c0'].substr(5, 6)) \
    .withColumn('WBAN', df['_c0'].substr(11, 5)) \
    .withColumn('ObservationDate', to_date(df['_c0'].substr(16, 8), 'yyyyMMdd')) \
    .withColumn('ObservationHour', df['_c0'].substr(24, 4).cast(IntegerType())) \
    .withColumn('Latitude', df['_c0'].substr(29, 6).cast('float') / 1000) \
    .withColumn('Longitude', df['_c0'].substr(35, 7).cast('float') / 1000) \
    .withColumn('Elevation', df['_c0'].substr(47, 5).cast(IntegerType())) \
    .withColumn('WindDirection', df['_c0'].substr(61, 3).cast(IntegerType())) \
    .withColumn('WDQualityCode', df['_c0'].substr(64, 1).cast(IntegerType())) \
    .withColumn('SkyCeilingHeight', df['_c0'].substr(71, 5).cast(IntegerType())) \
    .withColumn('SCQualityCode', df['_c0'].substr(76, 1).cast(IntegerType())) \
    .withColumn('VisibilityDistance', df['_c0'].substr(79, 6).cast(IntegerType())) \
    .withColumn('VDQualityCode', df['_c0'].substr(86, 1).cast(IntegerType())) \
    .withColumn('AirTemperature', df['_c0'].substr(88, 5).cast('float') / 10) \
    .withColumn('ATQualityCode', df['_c0'].substr(93, 1).cast(IntegerType())) \
    .withColumn('DewPoint', df['_c0'].substr(94, 5).cast('float')) \
    .withColumn('DPQualityCode', df['_c0'].substr(99, 1).cast(IntegerType())) \
    .withColumn('AtmosphericPressure', df['_c0'].substr(100, 5).cast('float') / 10) \
    .withColumn('APQualityCode', df['_c0'].substr(105, 1).cast(IntegerType())).drop('_c0')

splitDF.printSchema()
splitDF.show(5)

# splitDF.write.format("csv").mode("overwrite").option("header", "true").save("s3a://vvittanala/90-uncompressed.csv")
# splitDF.write.format("csv").mode("overwrite").option("header", "true").option("compression", "lz4").save("s3a://vvittanala/90-compressed.csv")


# cols_df = splitDF.coalesce(1)
# cols_df.write.format("csv").mode("overwrite").option("header", "true").save("s3a://vvittanala/90.csv")

# splitDF.write.format("parquet").mode("overwrite").option("header", "true").save("s3a://vvittanala/90.parquet")

temp_df = splitDF.filter((splitDF["AirTemperature"] >= -40) & (splitDF["AirTemperature"] <= 50))
# created a temp_df variable to filter temperature values in human survival values

ymfilter_df = temp_df.select(
    year("ObservationDate").alias("year"),
    month("ObservationDate").alias("month"),
    "AirTemperature"
)
# selected a columns as ObservationDate, AirTemperture and extracted it into a year & month column

avg_df = ymfilter_df.groupBy("year", "month").agg(avg("AirTemperature").alias("avg_temperature"))

stdev_df = avg_df.agg(stddev("avg_temperature").alias("stddev_temperature"))


sdev_df = ymfilter_df.groupBy("year", "month").agg(stddev("AirTemperature").alias("stddev_temperature"))

# Join average and standard deviation DataFrames
joined_df = avg_df.join(sdev_df, ["year", "month"], "inner")

# Select the desired columns
result_df = joined_df.select("avg_temperature", "stddev_temperature", "month", "year")

# Show the resulting DataFrame
result_df.show(12)
result_df.write.format("parquet").mode("overwrite").option("header","true").save("s3a://vvittanala/part-three.parquet")



first_year_df = sdev_df.limit(12)

# Write to CSV file

first_year_df.write.format("csv").mode("overwrite").option("header","true").save("s3a://vvittanala/part3-three.csv")


# Show the first 12 records (for verification)
first_year_df.show(12)

spark.stop()