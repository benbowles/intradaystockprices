from imports import *
from deltahelper import *
import load_db


@lru_cache(maxsize=32)
def get_schedule(enddate=str(dt.datetime.now().date())):
    import imports
    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date=imports.START_TIME, end_date=enddate)
    return schedule


@lru_cache(maxsize=32)
def get_days_dict():
    schedule = get_schedule()
    d = dict(schedule.apply(lambda x: x['market_open'].timestamp(), axis=1))
    d = {str(k.date()): int(v) for k, v in d.items()}
    return d


def opening_seconds_for_day(date):
    day_to_openepoch = get_days_dict()
    return list(range(day_to_openepoch[date], day_to_openepoch[date] + 30 * 60, 1))


def backfill_opentime_forticker(ticker, d, verbose=True):
    df = get_delta_spark()
    tickerdf = df.filter(f"ticker == '{ticker}'").select('*').toPandas()
    start = True
    mega_df = []
    for n, (date, unix_start) in enumerate(d.items()):
        
        date_existing_df = tickerdf[tickerdf['Date'] == date]
        
        if len(date_existing_df) == 0:
            if verbose:
                print(f"There was no data for {ticker} aon {date}, ...")
                continue
        
        seconds = set(opening_seconds_for_day(date))
        if validate_need_backfill(date_existing_df, date):
            continue
        print(f"Adding data for {ticker} aon {date}, ...")
        
        df = load_db.get_df_from_vendor(ticker, unix_start, unix_start + 60 * 30)
        if verbose:
            print(f"Len Data before cleaning {len(df)} : {ticker}")
        final_df = load_db.create_cleaned_df(df)
        final_df['ticker'] = ticker
        if verbose:
            print(f"Len Data after cleaning {len(final_df)} : {ticker}")
        mega_df.append(final_df) 
        if len(mega_df) > (10 if start else 2000):
            print(f"Writing megadf morningg recods with len {len(mega_df)} dates of total dates {len(d.items())}")
            add_to_db(get_spark().createDataFrame(pd.concat(mega_df)), get_delta_spark())
            print("Finished writing records")
            mega_df = []
            start = False

    print(f"Almost done. Writing last records for {len(mega_df)} dates ... ")
    if mega_df:
        add_to_db(get_spark().createDataFrame(pd.concat(mega_df)), get_delta_spark())
            

def validate_need_backfill(datedf, date):
    seconds = set(opening_seconds_for_day(date))
    return len(set(datedf['Timestamp'].tolist()) & seconds) >= 30

    
def backfill_opentime_forticker_date(ticker, date, d, checkfirst=True, verbose=True):
    import load_db
    if checkfirst:
        df = get_delta_spark()
        datedf = df.filter(f"Date == '{date}' AND ticker == '{ticker}'").select('*').toPandas()
        if validate_need_backfill(datedf, date):
            if verbose:
                print(f"For date {date} and ticker {ticker}, already have opening 30 minutes...")
            return

    unix_start = d[date]
    print(f"UTC Timestamp start date of {unix_start} for {date}")
    df = load_db.get_df_from_vendor(ticker, unix_start, unix_start + 60 * 30)
    print(f"Len Data before cleaning {len(df)} : {ticker}")
    final_df = load_db.create_cleaned_df(df)
    final_df['ticker'] = ticker
    print(f"Len Data after cleaning {len(final_df)} : {ticker}")
    add_to_db(get_spark().createDataFrame(final_df), get_delta_spark())
    print("Finished writing records")


def backfill_opentime(verbose=True):

    tickers = open('tickers.txt').read().split('\n')
    start = True
    d = get_days_dict()
    
    for ticker in tickers:
        print(f"Starting ticker {ticker}")
        backfill_opentime_forticker(ticker, d)


        
def backfill_from_cache():
    daydict = get_days_dict()
    from load_db import create_cleaned_df
    mega_df = []
    start = True
    for f in glob.glob('cache/*')[10:]:

        try:
            if f.startswith("Ticker") or f.startswith("-") or 'QCOM' in f or 'SBUX' in f or 'PVH' in f :
                continue

            ticker_name = f.replace('cache/', '').split('-')[0]

            print("Starting " + ticker_name)

            df= pd.read_csv(f)
            df['Datetime'] = pd.to_datetime(df['Datetime'])
            df['Date'] = df['Datetime'].apply(lambda x: str(x.date()))
            df['ticker'] = ticker_name
            for date in daydict.keys():
                datedf = df[df["Date"] == date]
                if len(datedf) > 0:
                    restrictdf = datedf[(datedf['Timestamp'] >= daydict[date]) & (datedf['Timestamp'] <= (daydict[date] + 1800))]
                    restrictdf = create_cleaned_df(restrictdf, withprogress=False, verbose=False)
                    restrictdf['ticker'] = ticker_name
                    mega_df.append(restrictdf)

                if len(mega_df) > (10 if start else 3000):
                    print(f"Writing megadf morningg recods with len {len(mega_df)} dates of total dates {len(daydict.items())}")
                    add_to_db(get_spark().createDataFrame(pd.concat(mega_df)), get_delta_spark())
                    print("Finished writing records")
                    mega_df = []
                    start = False

                if mega_df and len(mega_df) % 100 == 0:
                    print("mega df is " + str(len(mega_df)))

            if len(mega_df) > 0:
                add_to_db(get_spark().createDataFrame(pd.concat(mega_df)), get_delta_spark())
                mega_df = []
        except:
            import traceback
            print(traceback.format_exc())
            
        print("Finished")
