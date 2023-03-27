from pandas.api.indexers import FixedForwardWindowIndexer


def add_max_rolling(df, window_size, col_name):
    idx = FixedForwardWindowIndexer(window_size=window_size)
    df['max_rolling_mean_in_front'] = df['Close'].shift(-1).rolling(window=idx).max()
    df[col_name] = df['max_rolling_mean_in_front'] - df['Close']



# data = [("Alice", 1), ("Bob", 2), ("Charlie", 3), ("David", 4), ("Eve", 5)][::-1]
# columns = ["name", "age"]
# df = spark.createDataFrame(data, columns)