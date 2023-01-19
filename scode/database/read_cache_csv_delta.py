import os.path
import sys

from ..imports import *
from . import sparkdf
from . import preprocessing
from . import helpers
from . import constants

schema_in = StructType([
    StructField("Index", IntegerType(), False),

    StructField("Timestamp", IntegerType(), False),
    StructField("Gmtoffset", IntegerType(), False),
    StructField("Datetime", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
])

schema_out = StructType([
    StructField("Timestamp", DoubleType(), False),
    StructField("Volume", DoubleType(), True),
    StructField("Date", StringType(), True),
    StructField("Datetime", TimestampType(), True),
    StructField("ticker", StringType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Close_o", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Low_o", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("High_o", DoubleType(), True),
    StructField("Open", DoubleType(), True),
    StructField("Open_o", DoubleType(), True),
    StructField("interpolate", BooleanType(), True),
])

name_to_type = {
    "Timestamp": int,
    "Volume": float,
    "Date": str,
    "Datetime": pd._libs.tslibs.timestamps.Timestamp,
    "ticker": str,
    "Close": float,
    "Close_o": float,
    "Low": float,
    "Low_o": float,
    "High": float,
    "High_o": float,
    "Open": float,
    "Open_o": float,
    "interpolate": bool
}
cols = [f.name for f in schema_out.fields]

def validate_all(df):

    def validate_return_df(df, column, obj_type):
        return_vals = []
        date = df['Date'].tolist()[0]
        found = 0
        for value in df[column].tolist():

            if type(value) == obj_type:
                return_vals.append(value)
            else:
                print(f"{date} had a problem for {column}, it was not equal to {obj_type} it was equal to {str(value)}")
                return_vals.append(None)
                found += 1
        if found:
            print(df[column])
            print(f"For column {column} and  {obj_type} there was {found} problems")
        df[column] = return_vals
        return df

    for k, v in name_to_type.items():
        df = validate_return_df(df, k, v)
    return df


def process_ticker_date(pdf):

    from scode import clean_df, business_dates
    dates = business_dates()

    if str(pdf['Date'].tolist()[0]) not in dates:
        return pd.DataFrame(columns=cols)

    pdf = pdf.sort_values(by='Timestamp')

    pdf_preprocessed = clean_df(pdf, withprogress=False)
    pdf_preprocessed['Datetime'] = pdf_preprocessed.index
    pdf_preprocessed['Datetime'] = pd.to_datetime(pdf_preprocessed['Datetime'])
    pdf_preprocessed['Date'] = pdf_preprocessed['Datetime'].apply(lambda x: str(x.date()))

    if type(pdf_preprocessed) == pd.DataFrame and len(pdf_preprocessed) > 0:
        pdf_preprocessed = validate_all(pdf_preprocessed)
        pdf_preprocessed['Datetime'] = pd.to_datetime(pdf_preprocessed['Datetime'])
        to_return = pdf_preprocessed[cols]
        return to_return
    return pd.DataFrame(columns=cols)


def add_date(datetime):
    try:
        dt = pd.to_datetime(datetime)
        return str(dt.date())
    except:
        print(traceback.format_exc())
        return "date?"


def add_ticker(file_name):
    return pathlib.Path(file_name).stem.split('-')[0]



add_ticker_udf = udf(add_ticker, StringType())
add_date_udf = udf(add_date, StringType())


def transform_df(df):
    df = df.withColumn("ticker", add_ticker_udf(input_file_name()))
    df = df.withColumn('Date', add_date_udf(df['Datetime']))
    df = df.groupby("Date").applyInPandas(process_ticker_date, schema=schema_out)
    return df


def csv_to_delta(paths, spark):
    print(','.join(paths))
    df = spark.read.schema(schema_in).csv(paths, header=True)
    df = transform_df(df)
    df = df.repartition(50)
    df.write.format("delta").mode("append") \
        .partitionBy("ticker") \
        .save(constants.DB_LOCATION)


def folder_csv_to_delta(folder_dir, csv_number=25, onebatch=False):
    csv_paths = sorted(glob.glob(f'{folder_dir}/*'))
    spark = sparkdf.get_spark()
    for paths in helpers.chunked_iterable(csv_paths, csv_number):
        abs_paths = [str(pathlib.Path(path).absolute()) for path in paths]
        print(f"Starting path {str(abs_paths)}")
        csv_to_delta(abs_paths, spark)
        if onebatch:
            return
