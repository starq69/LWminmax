#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import (absolute_import, division, print_function, unicode_literals)
import logging, sys
#from I_InsideIndicator import InsideIndicator, New_InsideIndicator
#from I_OutsideIndicator import OutsideIndicator, ex_OutsideIndicator
import backtrader as bt
import backtrader.indicators as btind
from backtrader.utils.date import num2date # https://community.backtrader.com/topic/1151/datetime-format-internally/3

import pandas as pd
import numpy as np

'''
def isNaN(num):
    return num != num
'''

class LWminmaxIndicator(bt.Indicator):

    lines       =('LW_max', 'LW_min', 'LW_max_inter', 'LW_min_inter', 'inside')
    #params      = dict(inside=New_InsideIndicator, outside=OutsideIndicator)
    #params = (('lookback',1),)
    plotinfo    = dict(subplot=False)
    plotlines   = dict(
        LW_max      = dict(marker='^', markersize=8.0, color='green', fillstyle='full'),
        LW_min      = dict(marker='v', markersize=8.0, color='red', fillstyle='full'),
        LW_max_inter= dict(marker='^', markersize=16.0, color='blue', fillstyle='full'),
        LW_min_inter= dict(marker='v', markersize=16.0, color='black', fillstyle='full'),
    )

    _name = 'LWminmax'

    def __init__(self, strategy=None):

        self.log        = logging.getLogger (__name__)

        if strategy is not None and isinstance(strategy, bt.Strategy):
            print('LWminmax indicator attached to strategy')
            self.strategy = strategy
        else:
            self.log.error('Pls link the indicator to Strategy')
            sys.exit(1)

        self.prev       = self.min_max_inter_flag = 0
        self.ref_min    = self.ref_max = self.lookback = 1
        self.c_inter_max= self.c_inter_min = 0
        self.test_max   = []
        self.test_min   = []

        self.test_ref_max= []
        self.test_ref_min= []

        # da rimuovere (per ora serve ad attivare prenext())
        self.down       = bt.And(self.data.low(0) < self.data.low(-self.lookback), self.data.high(0) <= self.data.high(-self.lookback))

        #super(LWminmaxIndicator, self).__init__()


    def report_dataframe(self):
        '''
        invocata nella next() sull'ultimo datapoint
        '''

        self.pdf = pd.DataFrame() # può essere locale

        _len = len(self.data)

        print('***' + self.data._name + '***')

        #self.pdf['float_dt']        = self.data.datetime.get(size=_len) ## KEY
        # # https://community.backtrader.com/topic/1151/datetime-format-internally/3

        self.pdf['datetime']        = [self.data.num2date(_internal_date).strftime('%d-%m-%Y') for _internal_date in self.data.datetime.get(size=_len)]

        self.pdf['LW_max']          = self.LW_max.get(size=_len)
        #self.pdf['LW_max']          = self.pdf['LW_max'].apply(pd.to_numeric, downcast='float') 
        self.pdf['LW_max']          = self.pdf['LW_max'].replace(np.nan,0).apply(pd.to_numeric, downcast='float') 

        self.pdf['LW_min']          = self.LW_min.get(size=_len)
        #self.pdf['LW_min']          = self.pdf['LW_min'].apply(pd.to_numeric, downcast='float') 
        self.pdf['LW_min']          = self.pdf['LW_min'].replace(np.nan, 0).apply(pd.to_numeric, downcast='float') 

        self.pdf['LW_max_inter']    = self.LW_max_inter.get(size=_len)
        #self.pdf['LW_max_inter']    = self.pdf['LW_max_inter'].apply(pd.to_numeric, downcast='float') 
        self.pdf['LW_max_inter']    = self.pdf['LW_max_inter'].replace(np.nan, 0).apply(pd.to_numeric, downcast='float') 

        self.pdf['LW_min_inter']    = self.LW_min_inter.get(size=_len)
        #self.pdf['LW_min_inter']    = self.pdf['LW_min_inter'].apply(pd.to_numeric, downcast='float') 
        self.pdf['LW_min_inter']    = self.pdf['LW_min_inter'].replace(np.nan, 0).apply(pd.to_numeric, downcast='float') 

        # per convertire a int debbo prima sostituire NaN con 0
        self.pdf['inside']          = self.inside.get(size=_len)
        self.pdf['inside']          = self.pdf['inside'].replace(np.nan, 0).astype('int16', errors='ignore')

        self.pdf.set_index('datetime', inplace=True)

        self.strategy.indicators[self._name][self.data._name]['output_dataframe'] = self.pdf ### NEW


    def prenext(self):
        self.log.info(self.data.datetime.datetime().strftime('%d-%m-%Y')+ ' PRENEXT --> low: ' + repr(self.data.low[0]) + ', high:' + repr(self.data.high[0]))


    def eval_outside(self, msg):
        '''da ottimizzare in quanto non esiste il caso self.ref_min/max == 0
        '''
        msg += 'outside, '
        if self.ref_max > self.ref_min:
            if self.ref_min > 0:
                if self.data.low <= self.data.low[-self.ref_min]:
                    msg += 'min incluso, '
                    self.ref_min = 1 
                    self.ref_max += 1
                else:
                    msg += 'min NON incluso: min & max +=1, '
                    self.ref_min += 1
                    self.ref_max += 1
            else:
                if self.data.high >= self.data.high[-self.ref_max]:
                    msg += 'max incluso: UNCOVERED(1) ref_min <= 0!, '
                    self.ref_max = 1 
                else:
                    msg += ' UNCOVERED(1), high[0]=' + str(self.data.high[0]) + ', high[-1]=' + \
                            str(self.data.high[-1]) + ', low[0]=' + str(self.data.low[0]) + \
                            ', low[-1]=' + str(self.data.low[-1])

        elif self.ref_max < self.ref_min:
            if self.ref_max > 0:
                if self.data.high >= self.data.high[-self.ref_max]:
                    msg += 'max incluso, '
                    self.ref_max = 1 
                    self.ref_min += 1
                else:
                    msg += 'max NON incluso: min & max +=1, '
                    self.ref_min += 1
                    self.ref_max += 1
            else:
                if self.data.low <= self.data.low[-self.ref_min]:
                    msg += 'min incluso: UNCOVERED(2) ref_maxi <= 0!, '
                    self.ref_min = 1 
                else:
                    msg += ' UNCOVERED(2), high[0]=' + str(self.data.high[0]) + ', high[-1]=' + \
                            str(self.data.high[-1]) + ', low[0]=' + str(self.data.low[0]) + \
                            ', low[-1]=' + str(self.data.low[-1])

        else:
            # sia ref_min sia ref_max sono inclusi (sono uguali solo all'inizio)
            #
            self.ref_min = self.ref_max = 1 

        return msg


    def _inside(self, lookback):
        return (self.data.high[0] <= self.data.high[-lookback] and self.data.low[0]  >= self.data.low[-lookback])

    def _outside(self, lookback):
        return (self.data.high[0] > self.data.high[-lookback] and self.data.low[0] < self.data.low[-lookback])

    def _up(self, lookback):
        return (self.data.high[0] > self.data.high[-lookback] and self.data.low[0] >= self.data.low[-lookback])

    def _down(self, lookback):
        return (self.data.low[0] < self.data.low[-lookback]  and self.data.high[0] <= self.data.high[-lookback])


    def update_offset_idx(self, which=0, reset=False, offset=0): 
        # which: 0 ==> min & max, -1 ==> min, 1 ==> max
        #
        if not reset: ### decremento in base a which
            if which == 1:
                if self.c_inter_max != 0: self.c_inter_max -= 1 
            elif which == -1:
                if self.c_inter_min != 0: self.c_inter_min -= 1
            else:
                self.c_inter_max -= 1
                self.c_inter_min -= 1

                self.test_ref_max = [x-1 for x in self.test_ref_max] ## NEW
                self.test_ref_min = [x-1 for x in self.test_ref_min] ## NEW

        else: ## reset in base all'offset
            if which == 1: 
                self.c_inter_max = offset
            elif which == -1:
                self.c_inter_min = offset
            else:
                self.c_inter_max = self.c_inter_min = offset


    def check_intermediate_max(self, ref_max):
        self.test_ref_max.append(ref_max) 
        msg = ''
        if len(self.test_ref_max) == 3: 

            msg += 'IMAX=(' + str(self.LW_max[self.test_ref_max[0]]) + ', ' + \
                    str(self.LW_max[self.test_ref_max[1]]) + ', ' + \
                    str(self.LW_max[self.test_ref_max[2]])+')'

            if self.LW_max[self.test_ref_max[0]] < self.LW_max[self.test_ref_max[1]] and \
                    self.LW_max[self.test_ref_max[1]] > self.LW_max[self.test_ref_max[2]]:
                msg+=', <INTER MAX found> '

                if self.min_max_inter_flag < 0: 
                    msg += 'atteso min '
                    #
                    # trovato un nuovo max quando si attendeva un min...
                    # ...si confronta il precedente max con il nuovo...
                    #
                    if self.c_inter_max != 0 and self.LW_max[self.c_inter_max] < self.LW_max[self.test_ref_max[1]]: 
                        # il nuovo è maggiore ==> update_offset con ref_max (offset del nuovo)
                        msg += 'NEW OK, '
                        self.update_offset_idx(which=1, reset=True, offset=self.test_ref_max[1]) 
                    
                    for x in range(2): self.test_ref_max.pop(0) 

                else: ## flush min + aggiorna indice candidato max (c_inter_max)
                    msg += 'atteso max '
                    if self.c_inter_min != 0:
                        self.LW_min_inter[self.c_inter_min] = self.LW_min[self.c_inter_min]
                        msg += ' [flush('+str(self.LW_min_inter[self.c_inter_min])+', '+str(self.c_inter_min)+')] '

                    self.update_offset_idx(which=1, reset=True, offset=self.test_ref_max[1])
                    msg += '--> ' + str(self.LW_max[self.test_ref_max[1]]) + ', '
                    for x in range(2): self.test_ref_max.pop(0) 
                    self.min_max_inter_flag = -1 # set on ricerca minimo
            else:
                self.test_ref_max.pop(0) 
        return msg 


    def check_intermediate_min(self, ref_min):
        self.test_ref_min.append(ref_min) 
        msg = ''

        if len(self.test_ref_min) == 3: ## ed test_min
            msg += 'IMIN=(' + str(self.LW_min[self.test_ref_min[0]]) + ', ' + \
                    str(self.LW_min[self.test_ref_min[1]]) + ', ' + \
                    str(self.LW_min[self.test_ref_min[2]])+')'

            if self.LW_min[self.test_ref_min[0]] > self.LW_min[self.test_ref_min[1]] and \
                    self.LW_min[self.test_ref_min[1]] < self.LW_min[self.test_ref_min[2]]:
                msg+=' <INTER MIN found> '
                if self.min_max_inter_flag > 0: 
                    msg += 'atteso max '
                    # 
                    # trovato nuovo min quando si attendeva un max...
                    # ...si confronta il precedente inter.min con il nuovo...
                    #
                    if self.c_inter_min != 0 and self.LW_min[self.c_inter_min] > self.LW_min[self.test_ref_min[1]]: 
                        msg += 'NEW OK '
                        # ..il nuovo è minore ==> update_offset con ref_min (offset del nuovo)
                        #
                        self.update_offset_idx(which=-1, reset=True, offset=self.test_ref_min[1]) 

                    for x in range(2): self.test_ref_min.pop(0)

                else: # flush max (su self.lines.LW_inter_max) e aggiorna indice candidato min (c_inter_min)
                    if self.c_inter_max != 0:
                        self.LW_max_inter[self.c_inter_max] = self.LW_max[self.c_inter_max]
                        msg += ' [flush('+str(self.LW_max_inter[self.c_inter_max])+', '+str(self.c_inter_max)+')] '
                    #else:
                    #    msg += ' [NO flush!] '

                    msg += 'atteso min '
                    self.update_offset_idx(which=-1, reset=True, offset=self.test_ref_min[1]) 

                    msg += '--> ' + str(self.LW_min[self.test_ref_min[1]]) + ', '

                    for x in range(2): self.test_ref_min.pop(0)
                    self.min_max_inter_flag = 1 # set on ricerca max
            else:
                self.test_ref_min.pop(0)
        return msg 


    def next(self):

        msg = ''
        #self.log.info(self.data.datetime.datetime().strftime('%d-%m-%Y')+ ' low: ' + repr(self.data.low[0]) + ', high:' + repr(self.data.high[0]))

        if not self._inside(self.lookback):
            if not self._outside(self.lookback): 
                if self._up(self.lookback):  
                    msg += 'UP, '
                    if self.prev < 0: # down
                        msg += 'prev=down, '
                        if (self.ref_max != 0):
                            self.LW_max[-self.ref_max] = self.data.high[-self.ref_max] 
                            msg += self.check_intermediate_max(-self.ref_max)
                            msg += 'SET->LWmax=' + str(self.LW_max[-self.ref_max])
                            self.ref_min+= 1
                        else:
                            if self.ref_min != 0: self.ref_min +=1

                    elif self.prev > 0:
                        msg += 'prev=up, '
                        if self.ref_min !=0 : self.ref_min +=1
                    else:
                        self.ref_min +=1

                    self.ref_max = 1 # OK 
                    self.prev    = 1 # up

                elif self._down(self.lookback): 
                    msg += 'DOWN, '
                    if self.prev > 0: #up
                        msg += 'prev=up, '
                        if (self.ref_min != 0): 
                            self.LW_min[-self.ref_min] = self.data.low[-self.ref_min]
                            msg += self.check_intermediate_min(-self.ref_min)
                            msg += 'SET->LWmin=' + str(self.LW_min[-self.ref_min])
                            self.ref_max+= 1
                        else:
                            if self.ref_max != 0: self.ref_max +=1

                    elif self.prev < 0: #down
                        msg += 'prev=down, '
                        if self.ref_max !=0 : self.ref_max +=1
                    else:
                        self.ref_max +=1

                    self.ref_min = 1 # OK
                    self.prev    =-1 # down
            
            else: # outside
                msg += self.eval_outside(msg)

            self.lookback = 1

        else: # inside
            self.lookback += 1
            msg += 'inside, '
            if self.ref_min != 0: self.ref_min += 1
            if self.ref_max != 0: self.ref_max += 1
        
        #self.l.inside[0] = self.lookback - 1
        if self.lookback > 1: self.l.inside[0] = self.lookback - 1
 
        self.update_offset_idx(which=0, reset=False, offset=0)  

        msg += ' ref_min ='+str(self.ref_min)+', ref_max='+str(self.ref_max) + ', prev='+str(self.prev)

        ### LAST Datapoint
        #
        if len(self.data) == self.data.buflen():
            msg += ', LAST '
            if self.ref_max > self.ref_min: self.LW_max[-self.ref_max+1] = self.data.high[-self.ref_max+1]
            elif self.ref_max < self.ref_min: self.LW_min[-self.ref_min+1] = self.data.low[-self.ref_min+1]

            # debug (print self.lw_min/max)
            #self.log.info(self.test_min)
            #self.log.info(self.test_max)

            self.report_dataframe()

        self.log.info(self.data.datetime.datetime().strftime('%d-%m-%Y')+ ' - ' + msg + ', lookback='+str(self.lookback))

