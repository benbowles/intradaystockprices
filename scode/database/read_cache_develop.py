from ..imports import *
from . import helpers
from . import constants

MIN_ALLOWED_TS = 368

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


def get_inter_details(int_column):
    total_missing = int_column.sum()
    max_so_far = 0
    temp = 0

    for item in int_column.tolist():
        if item:
            temp += 1
            max_so_far = builtins.max(max_so_far, temp)
        else:
            temp = 0
    return max_so_far, total_missing

def get_n_missing_dates(df):
    all_business_dates = set(helpers.business_dates())
    found_dates = set(df['Date'].apply(lambda x: str(x)).tolist())

    min_date = pd.to_datetime(df['Date']).min().date()
    max_date = pd.to_datetime(df['Date']).max().date()

    n_missing = 0
    delta = max_date - min_date
    n_dates_in_range = 0
    for i in range(delta.days + 1):
        day = str(min_date + timedelta(days=i))
        if day in all_business_dates and day not in found_dates:
            n_missing += 1
        n_dates_in_range += 1

    return str(min_date), str(max_date), n_missing, n_missing / n_dates_in_range


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

    final = pd.concat([before_open, open_close, after_close])
    final = final.sort_index()
    return final


import os.path
import sys

from ..imports import *
from . import sparkdf
from . import preprocessing
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
            print(f"{df['ticker'].tolist()[0]} : For column {column} and  {obj_type} there was {found} problems")
        df[column] = return_vals
        return df

    for k, v in name_to_type.items():
        df = validate_return_df(df, k, v)
    return df


def process_ticker_date(pdf):
    from scode import clean_df, business_dates

    dates = business_dates()

    if str(pdf["Date"].tolist()[0]) not in dates:
        print(f"{pdf['Date'].tolist()[0]} Date not in list...")
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
        pdf_preprocessed["Datetime"] = pd.to_datetime(pdf_preprocessed["Datetime"])
        to_return = pdf_preprocessed[cols]
    else:
        to_return = pd.DataFrame(columns=cols)
    import random

    return to_return

