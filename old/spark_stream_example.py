def streaming_way():
    df = spark.readStream.csv('/Users/bowles/stocks/newcache/C*.csv', schema=schema_in, header=True)
    df = transform_df(df)
    query = df.writeStream.format("database"). \
        outputMode("append"). \
        option("checkpointLocation", '/Users/bowles/stocks/c/' + str(random.random())). \
        partitionBy("ticker"). \
        start('/Users/bowles/stocks/delta8')
    query.awaitTermination()