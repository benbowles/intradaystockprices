from ..imports import *
from .schema import schema_out
from .constants import DB_LOCATION
from delta.tables import DeltaTable
from pyspark.sql.functions import col

def get_delta():
    spark = get_spark()
    return DeltaTable.forPath(spark, DB_LOCATION)

def optimize():
    get_delta().optimize().executeCompaction()

def get_entire_spark_df():
    spark = get_spark()
    return spark.read.format("delta").load(DB_LOCATION)

def get_entire_spark_df_sample():
    spark = get_spark()
    df = spark.read.format("delta").load(DB_LOCATION)
    return df[df.ticker.isin(['GOOG', 'SBUX', 'ADBE', 'MSFT' , 'SPXS'])]


def date_range(df, upper=None, lower=None):
    if not lower:
        lower = "2010-01-01"
        
    if not upper:
        upper = "2025-01-01"
        
    dates = (lower, upper)
    return df.where(col('Datetime').between(*dates))
    
    
def compact_files():
    delta_table = get_delta()
    delta_table.optimize().executeCompaction()
    get_spark().conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    delta_table.vacuum(0)
    

def delete_all_tickers():
    delta_obj = get_delta()
    df = get_entire_spark_df()
    tickers = df.select('ticker').distinct().toPandas()['ticker'].tolist()
    delta_obj.delete(col("ticker").isin(tickers))
    delta_obj.vacuum(retentionHours=0)

def add_to_db(spark_df, delta_table):

    for spark_col in delta_table.columns:
        if spark_col not in spark_df.columns:
            spark_df = spark_df.withColumn(spark_col, lit(None))

    try:
        get_delta().alias("oldData").merge(
            spark_df.alias("newData"),
            "oldData.Timestamp = newData.Timestamp AND oldData.ticker = newData.ticker") \
            .whenNotMatchedInsertAll() \
            .execute()
    except delta.exceptions.ConcurrentAppendException:
        time.sleep(1)


def get_spark():
    spark = SparkSession. \
        builder. \
        appName('my-demo-spark-job'). \
        getOrCreate()

    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    spark.conf.set("spark.sql.streaming.schemaInference", "true")

    spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark.sparkContext.setLogLevel('INFO')

    return spark


def optimize():
    delta_table = get_delta()
    delta_table.optimize().executeCompaction()


def see_detail():
    result = get_spark().sql(f'DESCRIBE DETAIL delta.`{DB_LOCATION}`').show()
    print(result)


def repartition():
    df = get_entire_spark_df()
    df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("ticker") \
        .save("EXAMPLE")


def drop_duplicates():
    df = get_entire_spark_df()
    df = df.dropDuplicates(["Timestamp", "ticker"])
    df.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("ticker") \
        .save(DB_LOCATION)

def write_new():
    spark = get_spark()
    df = spark.createDataFrame([], schema=schema_out)
    df.select('*').write.format('delta') \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("ticker") \
        .save(DB_LOCATION)

def delete_column(column_name):
    spark = get_spark()
    spark.sql(
        f"""ALTER TABLE delta.`{DB_LOCATION}` SET TBLPROPERTIES (
       'delta.columnMapping.mode' = 'name',
       'delta.minReaderVersion' = '2',
       'delta.minWriterVersion' = '5')"""
    )
    spark.sql(f"ALTER TABLE delta.`{DB_LOCATION}` DROP COLUMN {column_name}")
