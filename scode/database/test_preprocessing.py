
from scode.imports import timezone


def interpolate():
    import pandas as pd
    import numpy as np

    # create a sample dataframe with a datetime index for a market trading day, with 600 rows
    index = pd.date_range('2021-11-01 13:10:00', periods=600, freq='T')
    data = {col: np.arange(1, 601) for col in ['Timestamp', 'Open', 'High', 'Low', 'Close']}
    data['ticker'] = 'blah'
    data['quality_pass'] = False
    pd.set_option('display.max_rows', 100)
    df = pd.DataFrame(data, index=index)
    df['datetime'] = df.index
    df['Timestamp'] = df['datetime'].apply(lambda x: x.replace(tzinfo=timezone.utc).timestamp())
    df['Date'] = '2021-11-01'
    index_list = df.sample(20).index

    df = df.drop(index_list)

    assert len(df.loc[ df.index[0]: df.index[0]]) == 1
    intdf = code.database.preprocessing.interpolate_mins(df)
    day_to_start = code.database.helpers.get_days_dict()
    start_time = day_to_start['2021-11-01']
    end_time = start_time + code.database.constants.TRADING_SECS

    assert len(intdf.loc[two_indexs[0]: two_indexs[0]]) == 1

    for dt in index_list:
        if end_time >= dt.replace(tzinfo=timezone.utc).timestamp() >= start_time:
            example_value1 = float(intdf.loc[dt: dt].Close)
            index_plus_one = dt + pd.Timedelta(minutes=1)
            example_value2 = float(intdf.loc[index_plus_one: index_plus_one].Close)
            assert example_value2 == (example_value1 + 1)
        else:
            assert len(intdf.loc[dt: dt]) == 0