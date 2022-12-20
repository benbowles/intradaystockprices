from imports import *

def get_delta():
    spark = get_spark()
    return DeltaTable.forPath(spark, "/Users/bowles/stocks/delta4")


def get_delta_spark():
    spark = get_spark()
    return spark.read.format("delta").load("/Users/bowles/stocks/delta4")


def add_to_db(spark_df, delta_table):
    for column in [column for column in delta_table.columns if column not in spark_df.columns]:
        spark_df = spark_df.withColumn(column, lit(None))
    get_delta().alias("oldData").merge(
        spark_df.alias("newData"),
        "oldData.Timestamp = newData.Timestamp AND oldData.ticker = newData.ticker") \
       .whenNotMatchedInsertAll() \
       .execute()
        
def get_spark():
    spark = SparkSession. \
        builder. \
        appName('my-demo-spark-job'). \
        getOrCreate()

    
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    spark = spark.newSession()
    return spark


def optimize():
    deltaTable = get_delta()
    deltaTable.optimize().executeCompaction()

    
def repartition():
    df = get_delta_spark()
    df.write.format("delta").mode("overwrite") \
      .option("overwriteSchema", "true") \
      .partitionBy("ticker") \
      .save("/Users/bowles/stocks/delta4") 
    
def dropduplicates():
    df = get_delta_spark()
    df.dropDuplicates("Timestamp","ticker")
    df.write.format("delta").mode("overwrite") \
      .option("overwriteSchema", "true") \
      .partitionBy("ticker") \
      .save("/Users/bowles/stocks/delta4") 