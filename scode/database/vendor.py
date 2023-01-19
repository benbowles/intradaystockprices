from ..imports import *
from . import sparkdf
from . import constants

def get_df_from_vendor(ticker, start_unix, end_unix):

    url = f'https://eodhistoricaldata.com/api/intraday/{ticker}.US?api_token=635b4abf541968.71569979&interval=1m&from={start_unix}&to={end_unix}'
    print("Getting Result at " + url)
    date = requests.get(url).content

    if date.decode() == 'You exceeded your daily API requests limit.  Please contact support@eodhistoricaldata.com':
        raise Exception('You exceeded your daily API requests limit.  Please contact support@eodhistoricaldata.com')

    df = pd.read_csv(BytesIO(date))
    return df


def get_dataframe_stock(ticker, start_date, end_date):
    def datestr_to_unix(date_str):
        date = dt.datetime.strptime(date_str, '%Y-%m-%d')
        return time.mktime(date.timetuple())

    def minus_120_days(date_str):
        date = dt.datetime.strptime(date_str, '%Y-%m-%d').date() - timedelta(120)
        return str(date)

    final_start_date = start_date
    mega_df = pd.DataFrame()
    iteration = 1

    while final_start_date < end_date:
        print(minus_120_days(str(end_date)), datestr_to_unix(str(end_date)))

        startd, endd = minus_120_days(str(end_date)), str(end_date)

        start_unix, end_unix = datestr_to_unix(startd), datestr_to_unix(endd)
        df = get_df_from_vendor(ticker, start_unix, end_unix)

        if len(df) > 0:
            mega_df = mega_df.append(df)
        else:
            print("No Results, exiting")
            break

        end_date -= timedelta(120)
        iteration += 1
    return mega_df


def save_mega_csv(ticker, start, cache_folder='.temp/vendor/'):

    end_date = dt.datetime.now().date()

    print("Getting df from Vendor")
    from_vendor = get_dataframe_stock(ticker, start, end_date)

    print("Drop Dups")
    from_vendor = from_vendor.drop_duplicates()

    print("Save Results")
    os.makedirs('.temp/vendor/', exist_ok=True)
    from_vendor.to_csv(f"{cache_folder}/{ticker}-{str(start)}-{str(end_date)}.csv")


def handle_ticker(ticker, cache_folder, load_diff_only):

    if load_diff_only:
        start = max_date_in_db(ticker)
        if not start:
            start = constants.START_TIME_DT
        print(f"Using Start date - {start}")
    else:
        start = constants.START_TIME_DT

    print(f"Starting at date - {str(start)} for {ticker}")
    try:
        save_mega_csv(ticker, start, cache_folder=cache_folder)
    except:
        print(traceback.format_exc())


def run_2021_test():
    handle_ticker('GMFI', 'tempfolder', False)

def max_date_in_db(ticker, mintrue=False):
    def convert_to_dt(date_str):
        if type(date_str) == str:
            date_str = dt.datetime.strptime(date_str, '%Y-%m-%d').date()
        return date_str

    df = sparkdf.get_entire_spark_df()
    ticker = df.filter(df.ticker == ticker)

    dates = [convert_to_dt(row.Date)
             for row in ticker.select(ticker.Date).distinct().collect()]
    if len(dates) > 0:
        func = builtins.max if not mintrue else builtins.min
        max_date = func(dates)
        return max_date
    return None

def get_tickers():
    TICKER_PATH = pathlib.Path(__file__).parent.parent.parent.absolute() / 'ticker.txt'
    return open(TICKER_PATH).read().split('\n')

def save_all_ticker_csvs(cache_folder, load_diff_only=True):
    tickers = get_tickers()
    for ticker in tickers:
        handle_ticker(ticker, cache_folder, load_diff_only)

def save_all_ticker_csvs_parallel(temp_folder='.temp/vendor/', load_diff_only=True) -> object:
    tickers = get_tickers()
    print(f"Starting with {len(tickers)} tickers")
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
        future_to_url = {executor.submit(handle_ticker, ticker, temp_folder, load_diff_only): ticker
                         for ticker in tickers}
        for future in concurrent.futures.as_completed(future_to_url):
            data = future.result()
