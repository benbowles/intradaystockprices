from ..imports import *

schema_in = StructType([
    StructField("Index", IntegerType(), False),
    StructField("Timestamp", DoubleType(), False),
    StructField("Gmtoffset", FloatType(), False),
    StructField("Datetime", TimestampType(), False),
    StructField("Open", FloatType(), False),
    StructField("High", FloatType(), False),
    StructField("Low", FloatType(), False),
    StructField("Close", FloatType(), False),
    StructField("Volume", FloatType(), False),
])

schema_out = StructType([
    StructField("Timestamp", IntegerType(), False),
    StructField("Volume", IntegerType(), True),
    StructField("Date", StringType(), True),
    StructField("Datetime", TimestampType(), True),
    StructField("ticker", StringType(), True),
    StructField("Close", FloatType(), True),
    StructField("Close_o", FloatType(), True),
    StructField("Low", FloatType(), True),
    StructField("Low_o", FloatType(), True),
    StructField("High", FloatType(), True),
    StructField("High_o", FloatType(), True),
    StructField("Open", FloatType(), True),
    StructField("Open_o", FloatType(), True),
    StructField("interpolate", BooleanType(), True),
])