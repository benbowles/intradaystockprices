from scode.imports import *
import prepare_data
from deltahelper import *

def get_df_from_vendor(ticker, start_unix, end_unix):
    
    url = f'https://eodhistoricaldata.com/api/intraday/{ticker}.US?api_token=635b4abf541968.71569979&interval=1m&from={start_unix}&to={end_unix}'
    print("Getting Result at " + url)
    date = requests.get(url).content

    if date.decode() == 'You exceeded your daily API requests limit.  Please contact support@eodhistoricaldata.com':
        raise Exception('You exceeded your daily API requests limit.  Please contact support@eodhistoricaldata.com')

    df = pd.read_csv(BytesIO(date))
    return df

    
def get_dataframe_stock(ticker, start_date, end_date, existing_ticker_dates=None):

    def datestr_to_unix(date_str):
        date = dt.datetime.strptime(date_str, '%Y-%m-%d')
        return time.mktime(date.timetuple())

    def minus_120_days(date_str):
        date = dt.datetime.strptime(date_str ,'%Y-%m-%d').date() - timedelta(120)
        return str(date)

    final_start_date = start_date
    mega_df = pd.DataFrame()
    iteration = 1
    
    while final_start_date < end_date:
        print(minus_120_days(str(end_date)), datestr_to_unix(str(end_date)))
        
        startd, endd = minus_120_days(str(end_date)), str(end_date)
        
        skip_because_exist = existing_ticker_dates and ticker_all_dates_in_db(existing_ticker_dates, startd, endd)

        if not skip_because_exist:
            print(f"Not Skipping this batch, w ticker_all_dates_in_db was {skip_because_exist} ,  {startd}, {endd}")
            start_unix, end_unix = datestr_to_unix(startd), datestr_to_unix(endd)
            df = get_df_from_vendor(ticker, start_unix, end_unix)

            if len(df) > 0:
                mega_df = mega_df.append(df)
                
            else:
                print("No Results, exiting")
                break
        else:
            print(f"All of the dates we were expected to find, we already have, starting at {startd} ending at {endd}")
        end_date -= timedelta(120) 
        iteration += 1
    return mega_df


def create_cleaned_df(df, withprogress=False, verbose=False):

    try:
        df['Datetime'] = pd.to_datetime(df['Datetime'])
    except:
        print(df['Datetime'])
        raise
    df['Date'] = df['Datetime'].apply(lambda x: str(x.date()))
    df = df.set_index('Datetime')
    del df['Gmtoffset']
    
    pd.options.mode.chained_assignment = None
    dates = list(set(df['Date']))
    if verbose:
        print(f"In create cleaned df, found {len(dates)} records")
    date_list = set(business_dates())
    # import pdb; pdb.set_trace()
    
    datefs = []
    iterator = tqdm(dates) if withprogress else dates
    for date in iterator:
        if date in date_list:
            datefs.append(prepare_data.getdatedf(df, date) )
        
    # datedfs = [prepare_data.getdatedf(df, date) for date in tqdm(dates) if date in date_list]
    if datefs:
        final_df = pd.concat(datefs)
        final_df['Datetime'] = final_df.index
    # final_df.insert(0, 'Datetime', final_df['Datetime'])
    
        return final_df
    return pd.DataFrame()

def sort_raw_delta(datedf):
    datedf.set_index(pd.to_datetime(datedf['Datetime'],infer_datetime_format=True), inplace=True)
    datedf.sort_index(inplace=True)
    return datedf
    

def get_date_ticker(date, ticker):
    df = get_delta_spark()
    datedf = df.filter(f"Date == '{date}' AND ticker == '{ticker}'").select('*').toPandas()
    datedf = sort_raw_delta(datedf)
    return datedf
    
    

def max_date_in_db(ticker, df, mintrue=False):
    ticker = df.filter(df.ticker == ticker)
    dates = [row.Date for row in ticker.select(ticker.Date).distinct().collect()]
    if len(dates) > 0:
        func = max if not mintrue else min
        max_date = func([dt.datetime.strptime(date , '%Y-%m-%d').date() for date in dates])
        return max_date
    return None


def get_epoch_times(date):
    import calendar
    inputdt = dt.datetime.strptime(date , '%Y-%m-%d').replace(hour=0, minute=0, second=0, microsecond=0)
    day_start = inputdt.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(hours=23)
    return calendar.timegm(day_start.timetuple()), calendar.timegm(day_end.timetuple())


def add_ticker_day_todb(date, ticker):
    start_unix, end_unix = get_epoch_times(str(date))
    add_ticker_unix_time_range(ticker, start_unix, end_unix)

    
def add_ticker_unix_time_range(ticker, start_unix, end_unix, verbose=True):
    
    df = get_df_from_vendor(ticker, start_unix, end_unix)
    if verbose:
        print(f"Len Data before cleaning {len(df)} : {ticker}")
    final_df = create_cleaned_df(df)
    final_df['ticker'] = ticker
    if verbose:
        print(f"Len Data after cleaning {len(final_df)} : {ticker}")
    if len(final_df) > 0:
        add_to_db(get_spark().createDataFrame(final_df), get_delta_spark())
    else:
        print("doing nothing, no data")
    
    
def get_all_for_ticker_pd(ticker):
    df = get_delta_spark()
    return df.filter(df.ticker == ticker).select('*').toPandas()
    
    
def ensure_ticker(ticker):
    tickerdf = get_all_for_ticker_pd(ticker)
    print("got all for ticker")
    dates = tickerdf['Date'].tolist()
    mindate = min(pd.to_datetime(tickerdf['Datetime']).dt.date)
    for n, date in enumerate(business_dates()):
        datedf = tickerdf[tickerdf['Date'] == date]
        if len(datedf) < 140:
            print(f"For ticker {ticker}, found missing date {date}, which had only {len(datedf)} records, re-adding...")
            add_ticker_day_todb(date, ticker)
        if n % 20 == 0:
            print(f"{n} for {ticker}")

            
def ensure_all_tickers():
    tickers = open('tickers.txt').read().split('\n')
    for n, ticker in enumerate(tickers):
        print(f"ensuring {ticker} ({n} of {len(tickers)}" )
        ensure_ticker(ticker)

        
def business_dates():
    import backfill_morning
    return backfill_morning.get_days_dict().keys()

def add_multiple_dates_to_db_ticker(ticker, dates):
    
    def cleandf(ticker, start_unix, end_unix, verbose=True):

        df = get_df_from_vendor(ticker, start_unix, end_unix)
        if verbose:
            print(f"Len Data before cleaning {len(df)} : {ticker}")
        if len(df) > 0:
            final_df = create_cleaned_df(df)
            final_df['ticker'] = ticker
            if verbose:
                print(f"Len Data after cleaning {len(final_df)} : {ticker}")
        return final_df 
    
    megadf = []
    start = True
    for n, date in enumerate(dates):
        
        print(f"For ticker {ticker}, adding {date},...")
        start_unix, end_unix = get_epoch_times(str(date))
        
        new_df = cleandf(ticker, start_unix, end_unix, verbose=True)
        print(f"Megadf len is {len(new_df)} for date {date}")
        megadf.append(new_df)
        
        if n and n % (3 if start else 10) == 0:
            megadf = pd.concat(megadf)
            print(f"{n} for {ticker} of {len(dates)} total, adding all recent to DB")
            print(f"Megadf len is {len(megadf)}")
            add_to_db(get_spark().createDataFrame(megadf), get_delta_spark())
            megadf = []
            start = False
            
    if len(megadf) > 0:
        add_to_db(get_spark().createDataFrame(megadf), get_delta_spark())
        

def ticker_all_dates_in_db(existing_ticker_dates, start, end_date):
    
    applicable_bus_dates = get_business_dates_between(start, end_date)
    existing_ticker_dates = set(existing_ticker_dates)
    
    for date in applicable_bus_dates:
        if date not in existing_ticker_dates:
            print(f"{date} not in DB")
            return False
    return True
    
    
def handle_ticker(ticker, start, delta_table):
    
    business_dates_ls=  business_dates()
    print(f"Found total len business dates: {len(business_dates_ls)}")
    tickerdf = get_all_for_ticker_pd(ticker)
    
    existing_ticker_dates = set(tickerdf['Date'])
    print(f"We have a total of {len(existing_ticker_dates) } dates in DB")

    prop = len(set(tickerdf['Date'])) / len(business_dates_ls)
    print(f"{prop} of total business dates  were found to be there...")

    if prop > 0.9:
        print("Adding missing dates one by one...")
        dates_toadd = set(business_dates_ls) - set(tickerdf['Date'])
        print(f"Adding {len(dates_toadd)} dates...")
        print('2012-11-17' + str('2012-11-17' in dates_toadd))
        add_multiple_dates_to_db_ticker(ticker, list(dates_toadd))
        
    else:
        print(f"Adding all for {ticker} in 120 chunks... ")
    
    
        end_date=dt.datetime.now().date()
        # allfortickerpd = get_all_for_ticker_pd(ticker)

        print("Getting df from Vendor")
        from_vendor = get_dataframe_stock(ticker,start, end_date, existing_ticker_dates)

        print("Drop Dups")
        from_vendor = from_vendor.drop_duplicates()

        print("Save Results")
        from_vendor.to_csv(f"cache/{ticker}-{str(start)}-{str(end_date)}.csv")

        print("Clean Results")
        if len(from_vendor) > 0:
            final_df = create_cleaned_df(from_vendor)
            final_df['ticker'] = ticker

            for chunk in list(chunked(range(len(final_df)), 500000)):
                temp_df = final_df.iloc[chunk[0]:chunk[-1], :]
                spark = get_spark()
                spark_df = spark.createDataFrame(temp_df)
                add_to_db(spark_df, delta_table)


def get_days_dict():
    schedule = get_schedule()
    d = dict(schedule.apply(lambda x: x['market_open'].timestamp(), axis=1))
    d = {str(k.date()): int(v) for k, v in d.items()}
    return d


def get_business_dates_between(start, end_date):

    import backfill_morning
    business_days = set(backfill_morning.get_days_dict().keys())
    start = dt.datetime.strptime(start , '%Y-%m-%d')
    end_date = dt.datetime.strptime(end_date , '%Y-%m-%d')
    
    delta = end_date - start 
    days = []
    for i in range(delta.days + 1):
        day = start + timedelta(days=i)
        if str(day.date()) in business_days:
            days.append(str(day.date()))
    return days

    
def main(load_diff_only=True):
    tickers = open('tickers.txt').read().split('\n')
    delta_table = get_delta()
    df = get_delta_spark()
    default_date = dt.datetime.strptime(START_DATE , '%Y-%m-%d').date()
    
    print('got database table')
    for ticker in tickers:
        
        if load_diff_only:
            start = max_date_in_db(ticker, df)
            if not start:
                start=default_date
            print(f"Using Start date - {start}")
                
        else:
            start =default_date

        print(f"Starting at date {str(start)} for {ticker}")
        try:
            handle_ticker(ticker, start, df)
        except:
            print(traceback.format_exc())
                    

if __name__ == '__main__':
    main()
