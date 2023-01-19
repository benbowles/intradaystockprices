import backfill_morning
from scode.imports import *


def get_hour_minutes(datedf):
    d = defaultdict(lambda : defaultdict(lambda : defaultdict(int)))
    ts = datedf.index.tolist()
    for i, t in enumerate(ts):
        if i > 0:
            # d[str(ts[i].hour) + ('above30' if ts[i].minute > 30 else 'below30')][(ts[i] - ts[i -1] ).seconds / 60] += 1
            d[ts[i].hour]['gaps'][(ts[i] - ts[i -1] ).seconds / 60] += 1
            d[ts[i].hour]['total_minutes'] += 1
    return d


def filter_good_hours(datedf):
    
    assert datedf.index.name == 'Datetime'
    date = datedf['Date'].tolist()[0]
    day_dict = backfill_morning.get_days_dict()
    start_time = day_dict[date]
    
    datedf.sort_values(['Timestamp'], inplace=True)
    
    date_ok = True
    count = 0
    ingap = False
    maxgap = -float("inf")
    
    for i, (ts, row) in enumerate(datedf.iterrows()):
        if i > 0:
            if (row['Timestamp'] > start_time) & (row['Timestamp'] < (start_time + (60 * 60 * 6.5))):
                count += 1
                if ingap:
                    gapmins = ((ts - last_ts ).seconds / 60)
                    maxgap = max(maxgap, gapmins)
                    if ingap and gapmins> 15:
                        print(f"Found gap of 6 for {date}")
                        date_ok = False
                        break
                ingap=True
            else:
                ingap=False
            last_row, last_ts = row, ts
                                                     
    if count < 368:
        date_ok = False
    # print(count, date_ok, maxgap)
    datedf['quality_pass'] = date_ok
    return datedf, date_ok


def interpolate_mins(datedf):

    assert type(datedf.index) == pd.core.indexes.datetimes.DatetimeIndex
    datedf.sort_index(inplace=True)
    datedf.drop_duplicates(inplace=True)
    datedf = datedf[~datedf.index.duplicated(keep='first')]

    date = datedf['Date'].tolist()[0]
    day_dict = backfill_morning.get_days_dict()
    start_time = day_dict[date]
    
    beforeopen = datedf[(datedf['Timestamp'] < start_time)]
    afterclose = datedf[(datedf['Timestamp'] > (start_time + (60 * 60 * 6.5)))]
    openclose = datedf[(datedf['Timestamp'] > start_time) & (datedf['Timestamp'] < (start_time + (60 * 60 * 6.5)))]
    
    ts = openclose.index.tolist()
    col_list = ['Timestamp', 'Open', 'High', 'Low', 'Close']
    
    for i, t in enumerate(ts):
        if i > 0:
            diff_min = (ts[i] - ts[i -1] ).seconds / 60
            if diff_min == 1:
                continue
            else:
                assert type(ts[i]) == pd._libs.tslibs.timestamps.Timestamp

                row_back = openclose.loc[ts[i -1]]
                row_front = openclose.loc[ts[i]]

                assert ts[i] > ts[i -1], f"{str(ts[i])} {str(ts[i-1])} problem with ts"

                cols_linspace = {}
                for col in col_list:
                    cols_linspace[col] = np.linspace(getattr(row_back, col), getattr(row_front, col), int(diff_min) + 2)[1:-1]

                new_row = {}
                for min_i in range(int(diff_min) - 1):
                    for col in col_list:
                        val = cols_linspace[col][min_i]
                        assert type(val) == np.float64, f"Val was {str(val)} with type {type(val)}"
                        new_row[col] = int(val) if col == 'Timestamp' else val
                    new_row['ticker'] = getattr(row_front, 'ticker')
                    new_row['quality_pass'] = getattr(row_front, 'quality_pass')

                    openclose.loc[ts[i -1] + pd.Timedelta(minutes=min_i + 1)] = new_row
                    

    final = pd.concat([beforeopen, openclose, afterclose])
    final = final.sort_index()
    return final


def clean_minutes(datedf):
    datedf = datedf.sort_index()
    datedf, date_ok = filter_good_hours(datedf)
    if date_ok:
        datedf = interpolate_mins(datedf)
    return datedf


def getdatedf(df, date):
    def switch_perc(column_title, firstclose):
        temp = copy.deepcopy(datedf[column_title])
        del datedf[column_title]
        datedf[column_title] = 100 * (temp -firstclose ) / firstclose
        datedf[column_title + '_o'] = temp
    
    datedf = df[df['Date'] == date]
    firstclose = float(datedf['Close'].head(1))
    switch_perc('Close', firstclose)
    switch_perc('Low', firstclose)
    switch_perc('High', firstclose)
    switch_perc('Open', firstclose)
    datedf = clean_minutes(datedf)
    return datedf

