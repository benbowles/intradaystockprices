import backfill_morning
from imports import * 


def get_hour_minutes(datedf):
    d = defaultdict(lambda : defaultdict(int))
    ts = datedf.index.tolist()
    for i, t in enumerate(ts):
        if i > 0:
            # d[str(ts[i].hour) + ('above30' if ts[i].minute > 30 else 'below30')][(ts[i] - ts[i -1] ).seconds / 60] += 1
            d[ts[i].hour][(ts[i] - ts[i -1] ).seconds / 60] += 1
    return d


def filter_good_hours(datedf):
    hour_minutes = get_hour_minutes(datedf)

    collected_hours = []

    start_hour = 15    
    while 1:
        if hour_minutes[start_hour][1.0] < 54:
            break
        collected_hours.append(start_hour)
        start_hour += 1

    start_hour = 15    
    while 1:
        if hour_minutes[start_hour][1.0] < 54:
            break
        collected_hours.append(start_hour)
        start_hour -= 1

    date = datedf['Date'].tolist()[0]

    start = time.time()
    minutes = [s for s in backfill_morning.opening_seconds_for_day(date) if s % 60 == 0]
    minutes = set(minutes)
    
    secdf = datedf.loc[lambda x: x.Timestamp.isin(minutes) ]

    datedf = pd.concat([datedf.loc[lambda x: x.index.hour.isin(collected_hours)], secdf])
    datedf = datedf.sort_index(ascending=True)
    return datedf


def interpolate_mins(datedf):

    ts = datedf.index.tolist()
    col_list = ['Timestamp', 'Open', 'High', 'Low', 'Close']
    for i, t in enumerate(ts):
        if i > 0:
            diff_min = (ts[i] - ts[i -1] ).seconds / 60
            if diff_min == 1:
                continue
            else:
                row_back = datedf.loc[ts[i -1]]
                row_front = datedf.loc[ts[i]]

                cols_linspace = {}
                for col in col_list:
                    cols_linspace[col] = np.linspace(getattr(row_back, col), getattr(row_back, col), int(diff_min) + 2)[1:-1]

                new_row = dict(copy.copy(row_back))
                for min_i in range(int(diff_min) - 1):
                    for col in col_list:
                        new_row[col] = cols_linspace[col][min_i]
                    datedf.loc[ts[i -1] + pd.Timedelta(minutes=min_i + 1)] = new_row

    datedf = datedf.sort_index()
    return datedf


def clean_minutes(datedf):
    s = time.time()
    datedf = datedf.sort_index()
    datedf = filter_good_hours(datedf)
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
    datedf['ground_truth'] = gaussian_filter1d(datedf['Close'].tolist(), 8)
    # datedf = addslope(datedf, 'ground_truth')
    return datedf


def addslope( datedf, predcol):
    slopes, rows = [], []
    for row in datedf.iterrows():
        rows.append(row)
        if len(rows) < 11:
            slopes.append(np.NaN)
            continue
        minutes = (rows[-1][0] - rows[-10][0]).seconds / 60
        slope_val = ((getattr(rows[-1][1], predcol) - getattr(rows[-10][1], predcol)) / minutes)* 1000
        slopes.append(slope_val)
    datedf['slope_ground_truth'] = slopes
    return datedf
    
    