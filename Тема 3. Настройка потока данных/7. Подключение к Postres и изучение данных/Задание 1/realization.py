import os

from pyspark.sql import SparkSession

# необходимые библиотеки для интеграции Spark с Kafka и Postgres
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# создаём SparkSession с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и Postgres
spark = SparkSession.builder \
    .appName("RestauranttSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()



df = spark.read \
            .format('jdbc') \
            .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
            .option('driver', 'org.postgresql.Driver') \
            .option('dbtable', 'marketing_companies') \
            .option('user', 'student') \
            .option('password', 'de-student') \
            .load()


df.count()