



import numpy as np
import pandas as pd
from pyspark.sql.types import *
import pandas as pd

from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType
import mplfinance as mpf
from pyspark.sql.functions import explode
from scipy.ndimage import gaussian_filter
from datetime import datetime


from delta.tables import *
import random


def add_feature3(func, column_name, delta_path):
    
    from pyspark.sql import SparkSession

    spark = SparkSession. \
        builder. \
        master("local[*]"). \
        appName('my-demo-spark-job2'). \
        getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


    deltaTable = DeltaTable.forPath(spark, delta_path)
    df = spark.read.format("delta").load(delta_path)

    @pandas_udf(f"Timestamp long, {column_name} double", PandasUDFType.GROUPED_MAP)
    def add_feature_udf(pdf):
        pdf = pdf.sort_values(by='Timestamp')
        dt, close = pdf["Timestamp"], pdf["Close"]
        assert sorted(dt.tolist()) == dt.tolist()
        pdf[column_name] = func(pdf)
        return pdf[['Timestamp', column_name]]


    newval = df.groupby("Date").apply(add_feature_udf)
    deltaTable.alias("oldData") \
      .merge(
        newval.alias("newData"),
        "oldData.Timestamp = newData.Timestamp") \
      .whenMatchedUpdateAll() \
      .execute()
    
    
def func(df):
    stream = False
    backwards_tip = df.tail(40)[::-1]
    index, first_row = next(backwards_tip.iterrows())
    rows = [first_row]
    
    for cnt, (index, row) in enumerate(backwards_tip.iterrows()):
        
        if row['gauss9'] > rows[-1]['gauss9']:
            return False
        
        rows.append(row)
        
    return True
    
    
    
def pattern_search2(func, column_name, delta_path):
    
    from pyspark.sql import SparkSession

    spark = SparkSession. \
        builder. \
        master("local[*]"). \
        appName('my-demo-spark-job2'). \
        getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")


    deltaTable = DeltaTable.forPath(spark, delta_path)
    df = spark.read.format("delta").load(delta_path)

    @pandas_udf(f"start long, end long", PandasUDFType.GROUPED_MAP)
    def add_feature_udf(pdf):
        
        pdf = pdf.sort_values(by='Timestamp')
        dt, close = pdf["Timestamp"], pdf["Close"]
        assert sorted(dt.tolist()) == dt.tolist()
        
        items = []
        rows = pdf.iterrows()

        count = 0
        
        while count < len(pdf):
                
            if count > 40:
                if func(pdf.iloc[:count, :]):
                    items.append({'start':  pdf.iloc[count-40, :]['Timestamp'],
                                  'end':  pdf.iloc[count, :]['Timestamp']})
                    count += 40
        
            count += 1
            
        return pd.DataFrame.from_records(items)

    newval = df.groupby("Date").apply(add_feature_udf)
    search_result = newval.toPandas()
    search_result['date'] = search_result['end'].apply(lambda x: str(datetime.utcfromtimestamp(x).date()))
    return search_result