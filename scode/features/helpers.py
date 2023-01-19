from ..imports import *
from ..database import *


def add_feature3(func, return_schema, delta_path):

    df = get_entire_spark_df()
    cols = [f.name for f in return_schema.fields]
    assert cols[0] == 'Timestamp' and cols[1] == 'ticker', "first two cols must be Timestamp and ticker"

    @pandas_udf(return_schema, PandasUDFType.GROUPED_MAP)
    def add_feature_udf(pdf):
        pdf = pdf.sort_values(by='Timestamp')
        dt, close = pdf["Timestamp"], pdf["Close"]
        assert sorted(dt.tolist()) == dt.tolist()
        pdf[column_name] = func(pdf)
        return pdf[[*cols]]

    newval = df.groupby("Date", "ticker").apply(add_feature_udf)

    deltaTable =
    deltaTable.alias("oldData") \
        .merge(
        newval.alias("newData"),
        "oldData.Timestamp = newData.Timestamp AND oldData.ticker = newData.ticker") \
        .whenMatchedUpdateAll() \
        .execute()


# Create table in the metastore
DeltaTable.createIfNotExists(spark) \
  .tableName("default.people10m") \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .execute()