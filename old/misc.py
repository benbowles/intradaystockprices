

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
    

def filter_good_hours2(datedf):
    hour_minutes = get_hour_minutes(datedf)

    def hour_no_good(hour):
        max_gap = max(list(hour_minutes[hour]['gaps'].keys()))
        return hour_minutes[hour]['total_minutes'] < 47 or max_gap > 4
        
    collected_hours = []

    start_hour = 15    
    while 1:
        if hour_no_good(hour):
            break
        collected_hours.append(start_hour)
        start_hour += 1

    start_hour = 15    
    while 1:
        if hour_no_good(hour)
            break
        collected_hours.append(start_hour)
        start_hour -= 1

    date = datedf['Date'].tolist()[0]
    minutes = [s for s in backfill_morning.opening_seconds_for_day(date) if s % 60 == 0]
    minutes = set(minutes)
    
    secdf = datedf.loc[lambda x: x.Timestamp.isin(minutes) ]

    datedf = pd.concat([datedf.loc[lambda x: x.index.hour.isin(collected_hours)], secdf])
    datedf = datedf.sort_index(ascending=True)
    return datedf


def fix_ts(df):
    ts = []
    for v in df['Timestamp'].tolist():
        if type(v) == np.ndarray:
            ts.append(v[0])
        else:
            ts.append(int(v))
    df['Timestamp'] = ts
    return df
