#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging, sys
import backtrader as bt
import backtrader.indicators as btind
import pandas as pd
import numpy as np
from backtrader.utils.date import num2date 

def get_indicator_class():
    return InsideIndicator


class InsideIndicator(bt.Indicator):
    lines = ('inside',)

    _name = 'I_InsideIndicator'     # DEVE corrispondere al nome modulo TODO : si può fare di meglio ?

    def __init__(self, strategy=None):

        self.log        = logging.getLogger (__name__)

        if strategy is not None and isinstance(strategy, bt.Strategy):
            self.strategy = strategy
        else:
            self.log.error('something goes wrong during {} init : pls check parameters')
            sys.exit(1)

        self.i = 1
        #super(InsideIndicator, self).__init__()
   

    def report_dataframe(self):
        '''
        invocata nella next() sull'ultimo datapoint
        '''

        pdf = pd.DataFrame() 
        _len = len(self.data)

        #pdf['float_dt']        = self.data.datetime.get(size=_len) ## KEY
        # # https://community.backtrader.com/topic/1151/datetime-format-internally/3

        pdf['datetime']        = [self.data.num2date(_internal_date).strftime('%d-%m-%Y') for _internal_date in self.data.datetime.get(size=_len)]
        pdf['inside']          = self.inside.get(size=_len)
        pdf['inside']          = pdf['inside'].replace(np.nan, 0).astype('int16', errors='ignore')

        pdf.set_index('datetime', inplace=True)
        self.strategy.indicators[self._name][self.data._name]['output_dataframe'] = pdf ### NEW


    def next(self):

        msg = ''

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
            msg += 'LAST'

            # qui debug solo x last datapoint
            self.log.debug(self.data.datetime.datetime().strftime('%d-%m-%Y')+ ' - ' + self.data._name + ' - ' + msg)


class New_InsideIndicator(bt.Indicator):
    lines =  ('inside',)
    params = (('period', 1),)

    def __init__(self):

        super(New_InsideIndicator, self).__init__()
    
    def next(self):

        inside_high       = self.data.high[0] <= self.data.high[- int(self.p.period)]
        inside_low        = self.data.low[0]  >= self.data.low[- int(self.p.period)]

        print ('New_InsideIndicator : ins_count --------> ' + str(self.p.period))
        if inside_high and inside_low:
            self.inside[0] = 1
        else: 
            self.inside[0] = 0
