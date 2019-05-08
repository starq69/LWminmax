#!/usr/bin/env python
# -*- coding: utf-8 -*-

import backtrader as bt
import backtrader.indicators as btind
import pandas as pd
import numpy as np
from backtrader.utils.date import num2date 


class BestReturnIndicator(bt.Indicator):

    lines = ('percentual_absolute_ranking',)
    _name = 'BestReturnIndicator'       ##
    _periods = dict()

    def __init__(self, strategy=None):

        if strategy is not None and isinstance(strategy, bt.Strategy):
            print(self._name + ' attached to strategy')
            self.strategy = strategy
        else:
            self.log.error('Pls link the indicator to Strategy')
            sys.exit(1)

        self.i = 1
        #super(InsideIndicator, self).__init__()
   

    def report_dataframe(self):
        '''
        invocata nella next() sull'ultimo datapoint
        '''
        self.pdf = pd.DataFrame() # può essere locale
        _len = len(self.data)

        #self.pdf['float_dt']        = self.data.datetime.get(size=_len) ## KEY
        # # https://community.backtrader.com/topic/1151/datetime-format-internally/3

        self.pdf['datetime']        = [self.data.num2date(_internal_date).strftime('%d-%m-%Y') for _internal_date in self.data.datetime.get(size=_len)]
        self.pdf['inside']          = self.inside.get(size=_len)
        self.pdf['inside']          = self.pdf['inside'].replace(np.nan, 0).astype('int16', errors='ignore')

        self.pdf.set_index('datetime', inplace=True)
        self.strategy.indicators[self._name][self.data._name]['output_dataframe'] = self.pdf ### NEW

    def prenext(self):

        # accesso alla prima candle della serie

        # _periods = json.load()
        pass


    def next(self):


        inside_high       = self.data.high[0] <= self.data.high[- int(self.i)] ### <
        inside_low        = self.data.low[0]  >= self.data.low[- int(self.i)]  ### >

        if inside_high and inside_low:
            self.i +=1
        else:
            self.i = 1
        
        self.inside[0] = self.i - 1

        ### LAST Datapoint
        #
        if len(self.data) == self.data.buflen():
            self.report_dataframe()     ###
