import numpy as np
import mplfinance as mpf
import pandas as pd


def getall_where_values(datedf, run, pred):
    where_values = np.array([False]* len(datedf)) 
    for bet in run.bets:
        graphdf   = pd.DataFrame(datedf.index)
        buy_date, sell_date = bet['bought_time'], bet['sold_time']
        where_values += pd.notnull(graphdf[ (graphdf>=buy_date) & (graphdf <= sell_date) ])['Datetime'].values
        
    return where_values


def plot_fillin(datedf, pred_col):
    y1values = datedf['Close'].values
    y2value  = datedf['Low'].min()

    # where_values = getall_where_values(datedf, runner, pred)

    plots = [mpf.make_addplot( datedf[pred_col], type='scatter')]
    fig, axlis = mpf.plot(datedf,type='line', figsize=(10, 5),  show_nontrading=True, 
             addplot=plots, returnfig=True,  fill_between=dict(y1=y1values,y2=y2value,where=where_values,alpha=0.5,color='g'))

