
from scode import *

df = get_entire_spark_df()
df = df.withColumn("max_15", df.salary.cast('float'))

df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("ticker") \
    .save(DB_LOCATION)

