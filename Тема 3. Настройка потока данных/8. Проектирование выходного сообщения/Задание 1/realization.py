from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType


def spark_init(test_name) -> SparkSession:
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

    spark = (
        SparkSession.builder.appName(test_name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.jars.packages", spark_jars_packages)
            .getOrCreate()
    )

    return spark


postgresql_settings = {
    'user': 'student',
    'password': 'de-student'
}


def read_marketing(spark: SparkSession) -> DataFrame:
    marketing_df = (spark.read
                    .format("jdbc")
                    .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de")
                    .option("dbtable", "marketing_companies")
                    .option("driver", "org.postgresql.Driver")
                    .options(**postgresql_settings)
                    .load())

    return marketing_df


kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";'
}


def read_client_stream(spark: SparkSession) -> DataFrame:
    schema = StructType([
        StructField("client_id", StringType()),
        StructField("timestamp", DoubleType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
    ])
    df = (spark.readStream.format('kafka')
          .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
          .option('kafka.security.protocol', 'SASL_SSL')
          .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
          .option('kafka.sasl.jaas.config',
                  'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";') \
          .option("subscribe", "student.topic.cohort14.mustdayker")
          .load()
          .withColumn('value', f.col('value').cast(StringType()))
          .withColumn('event', f.from_json(f.col('value'), schema))
          .selectExpr('event.*', 'offset')
          .withColumn('timestamp',
                      f.from_unixtime(f.col('timestamp'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
          .dropDuplicates(['client_id', 'timestamp'])
          .withWatermark('timestamp', '10 minutes')
          )

    return df


def join(user_df, marketing_df) -> DataFrame:
    """

    {
        "client_id": идентификатор клиента,
        "adv_campaign_id": идентификатор рекламной акции,
        "adv_campaign_description": описание рекламной акции,
        "adv_campaign_start_time": время начала акции,
        "adv_campaign_end_time": время окончания акции,
        "created_at": время создания выходного ивента,
        "offset": офсет
    }
    """
    return (user_df
            .crossJoin(marketing_df)
            .withColumn("adv_campaign_id", marketing_df.id)
            .withColumn("adv_campaign_name", marketing_df.name)
            .withColumn("adv_campaign_description", marketing_df.description)
            .withColumn("adv_campaign_start_time", marketing_df.start_time)
            .withColumn("adv_campaign_end_time", marketing_df.end_time)
            .withColumn("adv_campaign_point_lat", marketing_df.point_lat)
            .withColumn("adv_campaign_point_lon", marketing_df.point_lon)
            .withColumn("client_id", f.substring('client_id', 0, 6))
            .withColumn("created_at", f.lit(datetime.now()))
            .select('client_id',
                    'adv_campaign_id',
                    'adv_campaign_name',
                    'adv_campaign_description',
                    'adv_campaign_start_time',
                    'adv_campaign_end_time',
                    "adv_campaign_point_lat",
                    "adv_campaign_point_lon",
                    'created_at',
                    'offset')
            )


if __name__ == "__main__":
    spark = spark_init('join stream')
    client_stream = read_client_stream(spark)
    marketing_df = read_marketing(spark)
    result = join(client_stream, marketing_df)

    query = (result
             .writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .start())
    query.awaitTermination()