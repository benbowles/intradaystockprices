
from .sparkdf import *
from .constants import *
from ..imports import *

@lru_cache(maxsize=32)
def get_dates():
    df = get_entire_spark_df()
    dates = df.select("Date").distinct().toPandas()['Date'].tolist()
    return dates

def random_datedf(ticker):
    dates = get_dates()
    date_str = random.choice(dates)
    return get_day_ticker(date_str, ticker)


def get_day_ticker(date_str, ticker):
    df = get_entire_spark_df()
    datedf = df.filter(f"Date == '{date_str}' AND ticker == '{ticker}'").select('*').toPandas()
    datedf.set_index(pd.to_datetime(datedf['Datetime'], infer_datetime_format=True), inplace=True)
    datedf.sort_index(inplace=True)
    return datedf