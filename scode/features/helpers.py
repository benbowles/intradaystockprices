from ..imports import *
from ..database import *


def add_feature_wrapper(func, return_schema, column_name, test=False):
    from . import sparkdf
    df = sparkdf.get_entire_spark_df() if not test else sparkdf.get_entire_spark_df_sample()
    df = df.repartition(50)
    cols = [f.name for f in return_schema.fields]
    assert 'Timestamp' in cols and 'ticker' in  cols, "first two cols must be Timestamp and ticker"

    @pandas_udf(return_schema, PandasUDFType.GROUPED_MAP)
    def add_feature_udf(pdf):
        pdf = pdf.sort_values(by='Timestamp')
        dt = pdf["Timestamp"]
        assert sorted(dt.tolist()) == dt.tolist()
        func(pdf)
        return pdf[[*cols]]

    newval = df.groupby("Date", "ticker").apply(add_feature_udf)

    deltaTable = sparkdf.get_delta()
    deltaTable.alias("oldData") \
        .merge(
        newval.alias("newData"),
        "oldData.Timestamp = newData.Timestamp AND oldData.ticker = newData.ticker") \
        .whenMatchedUpdateAll() \
        .execute()

def myfunc(pdf):
    pdf['Close_scrap'] = pdf['Close']

def add_column_test():
    return_schema = StructType([
            StructField("Timestamp", IntegerType(), True),
            StructField("ticker", StringType(), True),
            StructField("Close_scrap", DoubleType(), True),
    ])
    add_feature_wrapper(myfunc, return_schema, 'Close_scrap', test=True)

# func =
#
# # Create table in the metastore
# DeltaTable.createIfNotExists(spark) \
#   .tableName("default.people10m") \
#   .addColumn("id", "INT") \
#   .addColumn("firstName", "STRING") \
#   .addColumn("middleName", "STRING") \
#   .addColumn("lastName", "STRING", comment = "surname") \
#   .addColumn("gender", "STRING") \
#   .addColumn("birthDate", "TIMESTAMP") \
#   .addColumn("ssn", "STRING") \
#   .addColumn("salary", "INT") \
#   .execute()