from ..imports import *
from . import helpers
from . import constants

MIN_ALLOWED_TS = 368
import time
from contextlib import contextmanager

class Timer:
    def __init__(self, name=None):
        self.name = name
        self.start_time = None
        self.elapsed_time = None

    def __enter__(self):
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.elapsed_time = time.perf_counter() - self.start_time
        if self.name:
            print(f"Time taken for '{self.name}': {self.elapsed_time:.4f} seconds")
        else:
            print(f"Time taken: {self.elapsed_time:.4f} seconds")
            
def setup_dt_fields(df):
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    df['Date'] = df['Datetime'].apply(lambda x: str(x.date()))
    df = df.set_index('Datetime')
    df = df.sort_index()
    return df

def clean_df(df, withprogress=False, verbose=False):

    df = setup_dt_fields(df)
    del df['Gmtoffset']

    assert len(set(df['Date'])) == 1, "This function processes one date only."
    assert df.sort_values(['Timestamp'])['Timestamp'].tolist() == df['Timestamp'].tolist(), "Not sorted by Timestamp?"

    input_date = list(set(df['Date']))[0]
    assert str(input_date) in helpers.business_dates(), f"{input_date} was not in the list"

    first_open = float(df['Open'].head(1))
    switch_perc(df, 'Close', first_open)
    switch_perc(df, 'Low', first_open)
    switch_perc(df, 'High', first_open)
    switch_perc(df, 'Open', first_open)
    df = interpolate_mins(df)
    return df


def switch_perc(df, column_title, first_open):
    temp = copy.deepcopy(df[column_title])
    del df[column_title]
    df[column_title] = 100 * (temp - first_open) / first_open
    df[column_title + '_o'] = temp


def determine_quality(datedf):
    input_date = datedf['Date'].tolist()[0]
    day_to_open_ts = helpers.get_days_dict()
    start_time = day_to_open_ts[str(input_date)]
    end_time = start_time + constants.TRADING_SECS
    def ts_int_to_pd(ts_int):
        return pd.Timestamp(dt.datetime.utcfromtimestamp(ts_int))

    start_time_dt, end_time_dt = ts_int_to_pd(start_time), ts_int_to_pd(end_time)

    date_ok = True
    missing_count = 0
    max_gap = 0
    open_close_df = datedf.loc[datedf.index >= start_time_dt]
    open_close_df = open_close_df.loc[open_close_df.index < end_time_dt]

    for i, (ts, row) in enumerate(open_close_df.iterrows()):
        if i > 0:
            gap_mins = ((ts - last_ts).seconds / 60)
            missing_len = (gap_mins - 1)
            missing_count += missing_len
            max_gap = builtins.max(max_gap, missing_len)
        last_row, last_ts = row, ts

    return max_gap, missing_count


def interpolate_mins(datedf):
    assert type(datedf.index) == pd.core.indexes.datetimes.DatetimeIndex
    datedf.sort_index(inplace=True)
    datedf.drop_duplicates(inplace=True)
    datedf = datedf[~datedf.index.duplicated(keep='first')]

    datedf['interpolate'] = False
    date = datedf['Date'].tolist()[0]
    day_dict = helpers.get_days_dict()
    start_time = day_dict[date]

    before_open = datedf[(datedf['Timestamp'] < start_time)]
    after_close = datedf[(datedf['Timestamp'] > (start_time + constants.TRADING_SECS))]
    open_close = datedf[(datedf['Timestamp'] >= start_time) & (datedf['Timestamp'] <= (start_time + constants.TRADING_SECS))]

    ts = open_close.index.tolist()
    col_list = ['Timestamp', 'Open', 'High', 'Low',  'Open_o', 'High_o', 'Low_o', 'Close', 'Volume', 'Close_o']

    for i, t in enumerate(ts):
        if i > 0:
            diff_min = (ts[i] - ts[i - 1]).seconds / 60
            if diff_min == 1:
                continue
            else:
                assert type(ts[i]) == pd._libs.tslibs.timestamps.Timestamp

                row_back = open_close.loc[ts[i - 1]]
                row_front = open_close.loc[ts[i]]

                assert ts[i] > ts[i - 1], f"{str(ts[i])} {str(ts[i - 1])} problem with ts"

                cols_linspace = {}
                for col in col_list:
                    front_val, back_val = getattr(row_front, col), getattr(row_back, col)
                    cols_linspace[col] = np.linspace(front_val, back_val,
                                                     int(diff_min) + 1)[1:-1]
                new_row = {}
                for min_i in range(int(diff_min) - 1):
                    for col in col_list:
                        val = cols_linspace[col][min_i]
                        assert type(val) == np.float64, f"Val was {str(val)} with type {type(val)}"
                        new_row[col] = int(val) if col == 'Timestamp' else val
                    new_row['ticker'] = getattr(row_front, 'ticker')
                    new_row['interpolate'] = True
                    open_close.loc[ts[i - 1] + pd.Timedelta(minutes=min_i + 1)] = new_row


    def convert_to_int(x):
        try:
            return int(x)
        except (ValueError, TypeError):
            return 0
    
    
    final = pd.concat([before_open, open_close, after_close])
    final = final.sort_index()
    final['Volume'] = final['Volume'].apply(convert_to_int)
    return final