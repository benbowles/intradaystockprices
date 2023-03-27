
import scode
from scode import *

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



p = ['/Users/bowles/stocks/.temp/vendor/2023-03-23-run1/WMB-2012-05-16-2023-03-23.csv']
spark = get_spark()

df = spark.read.schema(schema_in).csv(p, header=True)
pdf = df.toPandas()
pdf = pdf.loc[pdf.Datetime.dt.date.astype(str) == '2017-01-17']
len(pdf.drop_duplicates(['Timestamp']))
 
df = transform_df(df)
df = df.repartition(400)



df.write.format("delta").mode("append").partitionBy("ticker").save(
    constants.DB_LOCATION
)