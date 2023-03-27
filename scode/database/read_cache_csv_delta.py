import os.path
import sys
import random

from ..imports import *
from . import sparkdf
from . import preprocessing
from .preprocessing import Timer
from . import helpers
from . import constants
from .schema import schema_out, schema_in

name_to_type = {
    "Timestamp": int,
    "Volume": int,
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
    "interpolate": bool,
}
cols = [f.name for f in schema_out.fields]


def validate_all(df):
    def validate_return_df(df, column, obj_type):
        return_vals = []
        date = df["Date"].tolist()[0]
        found = 0
        for value in df[column].tolist():

            if type(value) == obj_type:
                return_vals.append(value)
            else:
                print(
                    f"{date} had a problem for {column}, it was not equal to {obj_type} it was equal to {str(value)}"
                )
                return_vals.append(None)
                found += 1
        if found:
            print(df[column])
            print(
                f"{df['ticker'].tolist()[0]} : For column {column} and  {obj_type} there was {found} problems"
            )
        df[column] = return_vals
        return df

    for k, v in name_to_type.items():
        df = validate_return_df(df, k, v)
    return df


def process_ticker_date(pdf):
    from scode import clean_df, business_dates

    dates = business_dates()

    if str(pdf["Date"].tolist()[0]) not in dates:
        return pd.DataFrame(columns=cols)

    pdf = pdf.sort_values(by="Timestamp")

    assert len(set(pdf["Date"])), "Only one date allowed"
    assert len(set(pdf["ticker"])), "Only one date allowed"

    pdf_preprocessed = clean_df(pdf, withprogress=False)

    pdf_preprocessed["Datetime"] = pdf_preprocessed.index
    pdf_preprocessed["Datetime"] = pd.to_datetime(pdf_preprocessed["Datetime"])

    pdf_preprocessed["Date"] = pdf_preprocessed["Datetime"].apply(
        lambda x: str(x.date())
    )

    if type(pdf_preprocessed) == pd.DataFrame and len(pdf_preprocessed) > 0:
        pdf_preprocessed = validate_all(pdf_preprocessed)

        to_return = pdf_preprocessed[cols]
    else:
        to_return = pd.DataFrame(columns=cols)

    return to_return


def add_date(datetime, file_name):
    import pandas as pd

    dt = pd.to_datetime(datetime)
    if dt == None or dt == 0.0:
        print(
            f"Failure to convert value ({datetime}) in datetime column into proper format for {file_name}"
        )
        return str(date(2000, 12, 31))
    return str(dt.date())


def add_ticker(file_name):
    return pathlib.Path(file_name).stem.split("-")[0]


add_ticker_udf = udf(add_ticker, StringType())
add_date_udf = udf(add_date, StringType())


@pandas_udf(StringType())
def to_upper(s: pd.Series) -> pd.Series:
    return s.dt.date.astype(str)

@pandas_udf(StringType())
def ticker_udf(s: pd.Series) -> pd.Series:
    return s.apply(lambda x: pathlib.Path(x).stem.split("-")[0])


def transform_df(df):
    df = (
        df.withColumn("Timestamp", df.Timestamp.cast(IntegerType()))
        .withColumn("Gmtoffset", df.Gmtoffset.cast(IntegerType()))
        .withColumn("Volume", df.Volume.cast(IntegerType()))
    )

    df = df.withColumn("input_file_name", input_file_name())
    df = df.withColumn("ticker", ticker_udf("input_file_name"))
    df = df.withColumn("Date", to_upper("Datetime"))

    df = df.groupby(["Date", "ticker"]).applyInPandas(
        process_ticker_date, schema=schema_out
    )
    return df


def csv_to_delta(paths, spark):
    print(",".join(paths))
    df = spark.read.schema(schema_in).csv(paths, header=True)

    df = transform_df(df)
    df = df.repartition(400)
    df.write.format("delta").mode("append").partitionBy("ticker").save(
        constants.DB_LOCATION
    )
    print(f"Saved batch to {constants.DB_LOCATION}")


def folder_csv_to_delta(folder_dir, csv_number=25, onebatch=False):
    csv_paths = sorted(glob.glob(f"{folder_dir}/*"))
    csv_paths.sort()
    csv_paths = [path for path in csv_paths if not "/-" in path]
    print("Getting Spark...")
    spark = sparkdf.get_spark()
    print("got spark...")
    go = False
    for n, paths in enumerate(helpers.chunked_iterable(csv_paths, csv_number)):

        # if any(['SPXL-' in path for path in paths]):
        #     go = True

        # if not go:
        #     continue
        # paths =[ '.temp/vendor/2023-03-23 22:00:45.652105/SPXL-2012-05-16-2023-03-23.csv']
        abs_paths = [str(pathlib.Path(path).absolute()) for path in paths]
        print(f"Starting path {str(abs_paths)}")
        csv_to_delta(abs_paths, spark)
        if onebatch:
            return
