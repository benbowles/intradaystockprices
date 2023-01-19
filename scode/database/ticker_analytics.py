from ..imports import *
from . import sparkdf

def ticker_analytics():
    df = sparkdf.get_entire_spark_df()
    df.groupBy("ticker").count()

# get summary


def get_ticker_date_summary(pdatedf):

    from scode import get_inter_details

    max_gap, missing_count = get_inter_details(pdatedf['interpolate'])
    quality = False if (max_gap > 13 or missing_count > 14) else True

    ticker_col = pdatedf['ticker']
    assert len(set(ticker_col)) == 1
    ticker = ticker_col.head(1)[0]
    date_col = pdatedf['Date']
    assert len(set(date_col)) == 1
    date = date_col.head(1)[0]

    result = {'ticker': ticker,
              'Date': date,
              'quality': quality,
              'maxGap': max_gap,
              'numInt': missing_count}

    return pd.DataFrame.from_records([result])

def get_ticker_summary(pdf):

    from scode import get_n_missing_dates
    pdf['Date'] = pd.to_datetime(pdf['Date']).apply(lambda x: x.date())

    ticker_col = pdf['ticker']
    assert len(set(ticker_col)) == 1
    ticker = ticker_col.head(1)[0]

    true_false_cnt = dict(pdf.groupby('quality').count()['ticker'])
    proportion_quality = (true_false_cnt.get(True) or 0) / ( (true_false_cnt.get(False) or 0) +  (true_false_cnt.get(True) or 0))

    missing_counts = dict(pdf.groupby('numInt').count()['ticker'])
    proportion_all_open_close = missing_counts[0] / builtins.sum(list(missing_counts.values()))

    # from scode import get_n_missing_dates
    min_date, max_date, n_missing, p_missing = get_n_missing_dates(pdf)
    result = {'ticker': ticker,
              'nDates': len(set(pdf['Date'])),
              'proportionQuality': proportion_quality,
              'proportionAllOpenClosePoints': proportion_all_open_close,
              'avgMaxGap': pdf['maxGap'].mean(),
              'minDate': min_date,
              'maxDate': max_date,
              'nMissingDates': n_missing,
              'propMissing': p_missing}

    return pd.DataFrame.from_records([result])

def get_ticker_date_analytics():

    ticker_date_schema = StructType([
        StructField("ticker", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("quality", BooleanType(), True),
        StructField("maxGap", IntegerType(), True),
        StructField("numInt", IntegerType(), True),
    ])
    ticker_date_analytics = df.groupby("ticker", "Date").applyInPandas(get_ticker_date_summary,
                                                                       schema=ticker_date_schema)
    return ticker_date_analytics

def get_ticker_summaries():
    print("summaries...")
    ticker_schema_out = StructType([
        StructField("ticker", StringType(), True),
        StructField("nDates", IntegerType(), True),
        StructField("proportionQuality", DoubleType(), True),
        StructField("proportionAllOpenClosePoints", DoubleType(), True),
        StructField("avgMaxGap", DoubleType(), True),
        StructField("minDate", StringType(), True),
        StructField("maxDate", StringType(), True),
        StructField("nMissingDates", IntegerType(), True),
        StructField("propMissing", DoubleType(), True)
    ])

    df = sparkdf.get_entire_spark_df()
    ticker_date_analytics = get_ticker_date_analytics()
    ticker_analytics =  ticker_date_analytics.groupby("ticker").applyInPandas(get_ticker_summary,
                                                                          schema=ticker_schema_out)
    # add ticker info
    stock_schema = StructType([
        StructField("ticker", StringType(), True),
        StructField("etf", IntegerType(), True),
        StructField("bear", IntegerType(), True)
    ])
    stocks = spark.read.schema(stock_schema).csv('stocks.csv', header=True)
    ticker_analytics = ticker_analytics.join(stocks,
                                             ticker_analytics.ticker == stocks.ticker,
                                             'left'
                                             ).dropDuplicates()
    return ticker_analytics

def get_filtered_ticker_summaries():
    df = get_ticker_summaries()
    return df.filter('propMissing<0.1')

#
#
# scheme_cols = [f.name for f in schema_out.fields]
#
# schema = StructType(
#     [StructField("idvalue", StringType(), True),
#      StructField("hour", LongType(), True),
#      StructField("epochtimestamp", IntegerType(), True)]
# )
#
#
# @F.pandas_udf(schema, F.PandasUDFType.GROUPED_MAP)
# def filter_data(pdf):
#     MIN_NIGHT_HOUR = 0
#     MAX_NIGHT_HOUR = 24
#     idvalue = pdf.idvalue
#     hour = pdf.hour
#     return pdf.query('hour > @MIN_NIGHT_HOUR & hour < @MAX_NIGHT_HOUR')
#
# df1.groupBy(
#     'idvalue'
# ).apply(
#     filter_data
# ).show()