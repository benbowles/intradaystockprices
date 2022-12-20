import pandas as pd
import random
import matplotlib.pyplot as plt
import copy
from datetime import datetime
import numpy as np
from multiprocessing import Pool
import mplfinance as mpf

class Runner(object):
    
    def __init__(self, datedf, predcol, date, start_min=None, inverse=False, verbose=False):
        self.bought = False
        self.bought_price = None
        self.bets = []
        self.verbose = verbose
        self.rows = []
        self.slopes = []
        self.predcol = predcol
        self.date = date
        self.inverse = inverse
        self.datedf = datedf
        self.start_min = start_min
    
    def sell(self, row, n):
        per_gain = 100*(row[1].Close_o - self.current_bet['bought_price']) / self.current_bet['bought_price']
        if self.verbose:
            print(f"Selling: at: {row[1].Close_o } with percent gain of: {per_gain} at TimeStamp {str(row[0])}\n")
        self.current_bet['sold_price'] = row[1].Close_o
        self.current_bet['per_gain'] = per_gain
        self.current_bet['sold_time'] = row[0]
        self.current_bet['row_n_sold'] = n
        self.bets.append(self.current_bet)

        self.current_bet = {}
        self.bought = False
        self.buffer = 0
    
    def addslope(self, row):
        self.rows.append(row)
        if len(self.rows) < 11:
            self.slopes.append(np.NaN)
            return
        minutes = (self.rows[-1][0] - self.rows[-10][0]).seconds / 60
        slope_val = ((getattr(self.rows[-1][1], self.predcol) - getattr(self.rows[-10][1], self.predcol)) / minutes)* 1000
        self.slopes.append(slope_val)
    
    def run(self):
        self.buffer = 0
        minute_before_buy = 10

        for n, row in enumerate(self.datedf.iterrows()):
            
            if self.start_min and (self.start_min > n):
                continue
                
            self.addslope(row)
            if self.bought:
                if (not self.inverse and self.slopes[-1] < 0) or (self.inverse and self.slopes[-1] > 0):
                    self.sell(row, n)
                    
            else:
                if (not self.inverse and self.slopes[-1] < 0) or (self.inverse and self.slopes[-1] > 0):
                    self.buffer = 0
                else:
                    self.buffer += 1
                    if self.buffer > minute_before_buy:
                        self.bought = True
                        self.current_bet = {}
                        self.current_bet['bought_price'] = row[1].Close_o
                        self.current_bet['bought_time'] = row[0]
                        self.current_bet['row_n_bought'] = n
                        if self.verbose:
                            print("Bought price: " + str(row[1].Close_o)      + ' TimeStamp ' + str(row[0]))
        
        if self.bought:
            self.sell(row, n)
        return self

    def sum_bets(self):
        return sum([bet['per_gain'] for bet in self.bets])

    
    def getall_where_values(self):
        where_values = np.array([False]* len(self.datedf)) 
        for bet in self.bets:
            graphdf   = pd.DataFrame(self.datedf.index)
            buy_date, sell_date = bet['bought_time'], bet['sold_time']
            where_values += pd.notnull(graphdf[ (graphdf>=buy_date) & (graphdf <= sell_date) ])['Datetime'].values

        return where_values


    def plot_fillin(self, col=None):
        if col:
            pred = col
        else:
            pred = self.predcol
            
        y1values = self.datedf['Close'].values
        y2value  = self.datedf['Low'].min()

        where_values = self.getall_where_values()

        plots = [mpf.make_addplot( self.datedf[pred], type='scatter')]
        fig, axlis = mpf.plot(self.datedf,type='line', figsize=(10, 5),  show_nontrading=True, 
                 addplot=plots, returnfig=True,  fill_between=dict(y1=y1values,y2=y2value,where=where_values,alpha=0.5,color='g'))


class Runner2(Runner):
    
        def addslope3(self, row):
            self.rows.append(row)
            if len(self.rows) < 11:
                self.slopes.append(np.NaN)
                return
            minutes = (self.rows[-1][0] - self.rows[-3][0]).seconds / 60
            slope_val = ((getattr(self.rows[-1][1], self.predcol) - getattr(self.rows[-3][1], self.predcol)) / minutes)* 1000
            self.slopes3.append(slope_val)
        
        def run(self):
            
            self.slopes3 = []
            for n, row in enumerate(self.datedf.iterrows()):
                if self.start_min and (self.start_min > n):
                    continue

                self.addslope(row)
                self.addslope3(row)
                if self.bought:
                    if (not self.inverse and (self.slopes[-1] < 0)) or (self.inverse and self.slopes[-1] > 0):
                        self.sell(row, n)

                else:
                    if np.mean(self.slopes[-10:]) > 5:
                        self.bought = True
                        self.current_bet = {}
                        self.current_bet['bought_price'] = row[1].Close_o
                        self.current_bet['bought_time'] = row[0]
                        self.current_bet['row_n_bought'] = n
                        if self.verbose:
                            print("Bought price: " + str(row[1].Close_o)      + ' TimeStamp ' + str(row[0]))
                
            if self.bought:
                self.sell(row, n)
            return self

    
    
def simulate(runObj, df, pred_column, max_run=None, multiprocess=False, start_min=None):
    
    assert pred_column in df.columns

    
    dates = df['Date'].tolist()
    random.shuffle(dates)
    dates = dates[:max_run]

    date_to_runners = {}
    if not multiprocess:
        for date in dates:
            datedf = df[df['Date'] == date]    
            dt = datetime.strptime(date, "%Y-%m-%d")
            date_to_runners[dt] = {
                'runner': runObj(datedf, pred_column, dt, inverse=False, start_min=start_min).run(), 
                'inverse_runner': runObj(datedf, pred_column, dt, inverse=True, start_min=start_min).run()
            }

    records=[]
    index = []
    for dt in date_to_runners.keys():
        records.append({'bull_sum_gain': date_to_runners[dt]['runner'].sum_bets(),
                        'bull_num_bets': len(date_to_runners[dt]['runner'].bets),
                        'bull_won': date_to_runners[dt]['runner'].sum_bets() > 0,
                        'date': dt,
                        'bear_won': date_to_runners[dt]['inverse_runner'].sum_bets() < 0,
                        'overall_won':( date_to_runners[dt]['runner'].sum_bets() + (-date_to_runners[dt]['inverse_runner'].sum_bets())) > 0,
                        'bear_sum_gain': date_to_runners[dt]['inverse_runner'].sum_bets(),
                        'bear_num_bets': len(date_to_runners[dt]['inverse_runner'].bets)})

        index.append(dt)

    day_summary = pd.DataFrame.from_records(records, index=index)
    day_summary['year'] = day_summary.index.year

    cols = {col: ['mean'] for col in ['bull_sum_gain', 'bull_num_bets', 'bull_won',
                                 'bear_sum_gain', 'bear_num_bets', 'bear_won',  'overall_won' ]}

    cols['bull_sum_gain'].append('min')
    cols['bear_sum_gain'].append('max')

    year_summary = day_summary.groupby('year').agg(cols)
    year_summary['size'] = day_summary.groupby('year').size()

    return year_summary, date_to_runners