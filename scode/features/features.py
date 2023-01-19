

def add_max_rolling(df, window_size):
    idx = FixedForwardWindowIndexer(window_size=window_size)
    df['max_rolling_mean_in_front'] = df['Close_adj'].shift(-1).rolling(window=idx).max()
    df['percent_change_' + str(window_size)] = (df['max_rolling_mean_in_front']
                                                - df['Close_adj']) / df['Close_adj'] * 100
    return df