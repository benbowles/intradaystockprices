from imports import *
import matplotlib.pyplot as plt
def get_spark():
    spark = SparkSession. \
        builder. \
        appName('my-demo-spark-job'). \
        getOrCreate()

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    spark = spark.newSession()
    return spark

def get_day_ticker(date_str, ticker):
    
    df = get_spark().read.format("delta").load("/Users/bowles/stocks/delta2")
    datedf = df.filter(f"Date == '{date_str}' AND ticker == '{ticker}'").select('*').toPandas()
    datedf.set_index(pd.to_datetime(datedf['Datetime'],infer_datetime_format=True), inplace=True)
    datedf.sort_index(inplace=True)
    return datedf


def random_datedf(ticker):
    df = get_spark().read.format("delta").load("/Users/bowles/stocks/delta2")
    dates = df.select("Date").distinct().toPandas()['Date'].tolist()
    date_str = random.choice(dates) 
    print(date_str)
    df = get_spark().read.format("delta").load("/Users/bowles/stocks/delta2")
    return get_day_ticker(date_str, ticker)


def graph_day(datedf, extracolumn, fillin=[]):
    import mplfinance as mpf
    from datetime import datetime

    def getall_where_values(datedf, time_tuples):

        where_values = np.array([False]* len(datedf)) 
        for start_time, end_time in time_tuples:
            if type(start_time) == int:
                start_time, end_time = datetime.utcfromtimestamp(start_time) ,datetime.utcfromtimestamp(end_time)
            graphdf   = pd.DataFrame(datedf.index)
            where_values += pd.notnull(graphdf[ (graphdf>=start_time) & (graphdf <= end_time) ])['Datetime'].values

        return where_values


    plt.style.use('ggplot')
    def plot_fillin(datedf, time_tuples):


        y1values = datedf['Close'].values
        y2value  = datedf['Low'].min()

        where_values = getall_where_values(datedf, time_tuples)
        fill_between=dict(y1=y1values,y2=y2value,where=where_values,alpha=0.5,color='g')
        fig, axlis = mpf.plot(datedf,
                              type='line', 
                              figsize=(30, 20),  
                              fill_between=fill_between,
                              show_nontrading=True, 
                              returnfig=True)

        ticks = pd.date_range(min(datedf.index), max(datedf.index), freq='20T')
        _ = axlis[1].xaxis.set_ticks(ticks)
        return axlis



    ax = plot_fillin(datedf, fillin)
        
    for cols in extracolumn:
        ax[0].plot(datedf[extracolumn], 'ko', marker='.', markersize=1)

        