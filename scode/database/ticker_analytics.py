from ..imports import *
from . import helpers
from . import sparkdf


def ticker_analytics():
    df = sparkdf.get_entire_spark_df()
    df.groupBy("ticker").count()


def get_n_missing_dates_for_ticker(df):
    all_business_dates = set(helpers.business_dates())
    found_dates = set(df["Date"].apply(lambda x: str(x)).tolist())

    min_date = pd.to_datetime(df["Date"]).min().date()
    max_date = pd.to_datetime(df["Date"]).max().date()

    date_gaps = 0
    delta = max_date - min_date
    n_dates_in_range = 0
    for i in range(delta.days + 1):
        day = str(min_date + timedelta(days=i))
        if day in all_business_dates and day not in found_dates:
            date_gaps += 1
        n_dates_in_range += 1

    return str(min_date), str(max_date), date_gaps, date_gaps / n_dates_in_range


def get_inter_details(interpolate_col):
    total_interpolated = interpolate_col.sum()
    max_gap_so_far = 0
    temp = 0

    for item in interpolate_col.tolist():
        if item:
            temp += 1
            max_gap_so_far = builtins.max(max_gap_so_far, temp)
        else:
            temp = 0
    return max_gap_so_far, total_interpolated


def get_ticker_date_analytics(df):
    def get_ticker_date_summary(pdatedf):
        assert len(set(pdatedf["ticker"])) == 1

        # fields are only interpolated if between trading hours
        max_gap, missing_count = get_inter_details(pdatedf["interpolate"])

        gap_pass = max_gap < 13
        missing_count_pass = missing_count < 14
        item_count_pass = len(pdatedf) > 390
        quality = gap_pass and missing_count_pass and item_count_pass

        ticker = pdatedf["ticker"].head(1).tolist()[0]

        assert len(set(pdatedf["Date"])) == 1
        date = pdatedf["Date"].head(1).tolist()[0]

        result = {
            "ticker": ticker,
            "Date": date,
            "quality": quality,
            "pass_max_gap": gap_pass,
            "pass_missing_count": missing_count_pass,            
            "pass_item_count": item_count_pass,
            "max_gap": max_gap,
            "missing_count": missing_count,
            "item_count": len(pdatedf),
        }

        return pd.DataFrame.from_records([result])

    ticker_date_schema = StructType(
        [
            StructField("ticker", StringType(), False),
            StructField("Date", StringType(), False),
            StructField("quality", BooleanType(), False),
            StructField("pass_max_gap", BooleanType(), False),
            StructField("pass_missing_count", BooleanType(), False),
            StructField("pass_item_count", BooleanType(), False),
            StructField("max_gap", IntegerType(), False),
            StructField("missing_count", IntegerType(), False),
            StructField("item_count", IntegerType(), False),
        ]
    )
    ticker_date_analytics = df.groupby("ticker", "Date").applyInPandas(
        get_ticker_date_summary, schema=ticker_date_schema
    )
    return ticker_date_analytics


def get_ticker_summary(pdf):
    assert len(set(pdf["ticker"])) == 1
    ticker = pdf["ticker"].head(1)[0]

    def summarize_bool_field(field):
        true_false_cnt = dict(pdf.groupby(field).count()["ticker"])

        return true_false_cnt.get(True, 0) / (
            true_false_cnt.get(False, 0) + true_false_cnt.get(True, 0)
        )

    pdf["Date"] = pd.to_datetime(pdf["Date"]).apply(lambda x: x.date())

    min_date, max_date, n_date_gaps, prop_date_gap = get_n_missing_dates_for_ticker(pdf)
    result = {
        "ticker": ticker,
        "n_dates": len(set(pdf["Date"])),
        "min_date": min_date,
        "max_date": max_date,
        "n_date_gaps": n_date_gaps,
        "prop_date_gap": prop_date_gap,
        
        "prop_quality": summarize_bool_field("quality"),
        "prop_pass_max_gap": summarize_bool_field("pass_max_gap"),
        "prop_pass_missing_count": summarize_bool_field("pass_missing_count"),
        "prop_pass_item_count": summarize_bool_field("pass_item_count"),
        
        "mean_quality": pdf["quality"].mean(),
        "mean_max_gap": pdf["max_gap"].mean(),
        "mean_missing_count": pdf["missing_count"].mean(),
        "mean_item_count": pdf["item_count"].mean(),
    }
    return pd.DataFrame.from_records([result])


def get_ticker_summaries(test=False, date_range=None):

    print("summaries...")
    df = (
        sparkdf.get_entire_spark_df()
        if not test
        else sparkdf.get_entire_spark_df_sample()
    )
    if date_range:
        df = sparkdf.date_range(df, lower=date_range[0], upper=date_range[1])

    df = df.repartition(50)
    ticker_date_analytics = get_ticker_date_analytics(df)

    ticker_analytics = ticker_date_analytics.groupby("ticker").applyInPandas(
        get_ticker_summary,
        schema=StructType(
            [
                StructField("ticker", StringType(), False),
                StructField("n_dates", IntegerType(), False),
                StructField("min_date", StringType(), False),
                StructField("max_date", StringType(), False),
                StructField("n_date_gaps", IntegerType(), False),
                StructField("prop_date_gap", DoubleType(), False),
                
                StructField("prop_quality", DoubleType(), False),
                StructField("prop_pass_max_gap", DoubleType(), False),
                StructField("prop_pass_missing_count", DoubleType(), False),
                StructField("prop_pass_item_count", DoubleType(), False),
                
                StructField("mean_quality", DoubleType(), False),
                StructField("mean_max_gap", DoubleType(), False),
                StructField("mean_missing_count", DoubleType(), False),
                StructField("mean_item_count", DoubleType(), False)
            ]
        ),
    )    
    # add ticker info
    spark = sparkdf.get_spark()
    stocks = spark.read.schema(
        StructType(
            [
                StructField("ticker", StringType(), True),
                StructField("etf", IntegerType(), True),
                StructField("bear", IntegerType(), True),
            ]
        )
    ).csv("stocks.csv", header=True)

    ticker_analytics = ticker_analytics.join(
        stocks, ticker_analytics.ticker == stocks.ticker, "left"
    ).dropDuplicates()
    return ticker_analytics


def get_filtered_ticker_summaries():
    df = get_ticker_summaries()
    return df.filter("propMissing<0.1")
