from dataclasses import dataclass

from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType, BooleanType


Device = StructType([
    StructField("type", StringType(), False),
    StructField("version", StringType(), True),
])


Technical = StructType([
    StructField("browser", StringType(), False),
    StructField("os", StringType(), False),
    StructField("lang", StringType(), False),
    StructField("network", StringType(), False),
    StructField("device", Device, False),
])


User = StructType([
    StructField("ip", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
])


Source = StructType([
    StructField("site", StringType(), False),
    StructField("api_version", StringType(), False),
])


Page = StructType([
    StructField("current", StringType(), False),
    StructField("previous", StringType(), True)
])


EventLog = StructType([
    StructField("visit_id", StringType(), False),
    StructField("user_id", LongType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("page", Page, False),
    StructField("source", Source, False),
    StructField("user", User, False),
    StructField("technical", Technical, False),
    StructField("keep_private", BooleanType(), False)
])
