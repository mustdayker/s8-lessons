from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType

TOPIC_NAME = 'student.topic.cohort14.mustdayker'

def spark_init() -> SparkSession:
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
        ]
    )
    return (SparkSession.builder
            .master("local")
            .appName('test stream')
            .config("spark.jars.packages", spark_jars_packages)
            .getOrCreate()
            )


def load_df(spark: SparkSession) -> DataFrame:
    return (spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
            .option("subscribe", TOPIC_NAME)        
            .option('kafka.security.protocol', 'SASL_SSL')
            .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
            .option('kafka.sasl.jaas.config',
                    'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";')
            .load())


def transform(df: DataFrame) -> DataFrame:
    schema = StructType([
        StructField("client_id", StringType()),
        StructField("timestamp", DoubleType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType())
    ])

    return (df
            .withColumn('value', f.col('value').cast(StringType()))
            .withColumn('event', f.from_json(f.col('value'), schema))
            .selectExpr('event.*')
            .withColumn('timestamp',
                        f.from_unixtime(f.col('timestamp'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType()))
            .withWatermark('timestamp', '10 minutes')
            .dropDuplicates(subset=['client_id', 'timestamp'])
            )


spark = spark_init()

source_df = load_df(spark)
output_df = transform(source_df)
output_df.printSchema()

query = (output_df
         .writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", False)
         .trigger(once=True)
         .start())
try:
    query.awaitTermination()
finally:
    query.stop()