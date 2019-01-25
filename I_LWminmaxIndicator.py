#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import (absolute_import, division, print_function, unicode_literals)
import logging
#from I_InsideIndicator import InsideIndicator, New_InsideIndicator
from I_OutsideIndicator import OutsideIndicator, ex_OutsideIndicator
import backtrader as bt
import backtrader.indicators as btind


class LWminmaxIndicator(bt.Indicator):

    lines       =('LW_max', 'LW_min')
    #params      = dict(inside=New_InsideIndicator, outside=OutsideIndicator)
    #params = (('lookback',1),)
    plotinfo    = dict(subplot=False)
    plotlines   = dict(
        LW_max=dict(marker='^', markersize=8.0, color='green', fillstyle='full'),
        LW_min=dict(marker='v', markersize=8.0, color='red', fillstyle='full')
    )

    def __init__(self):
        self.log = logging.getLogger (__name__)
        self.prev = 0
        self.ref_min = self.ref_max = self.lookback = 1

        #self.inside     = self.p.inside()
        #self.outside    = self.p.outside()
        #self.up         = bt.And(self.data.high(0) > self.data.high(-self.lookback), self.data.low(0) >= self.data.low(-self.lookback))
        self.down       = bt.And(self.data.low(0) < self.data.low(-self.lookback), self.data.high(0) <= self.data.high(-self.lookback))

        #super(LWminmaxIndicator, self).__init__()

    def prenext(self):
        self.log.info(self.data.datetime.datetime().strftime('%d-%m-%Y')+ ' PRENEXT --> low: ' + repr(self.data.low[0]) + ', high:' + repr(self.data.high[0]))


    def _outside_01(self, msg):
        msg += 'outside, '
        if self.ref_max > self.ref_min:
            if self.ref_min > 0:
                if self.data.low <= self.data.low[-self.ref_min]:
                    msg += 'min incluso, '
                    self.ref_min = 1 
                    self.ref_max += 1
                else:
                    msg += 'min NON incluso, TBD, '
                    self.ref_min += 1
                    self.ref_max += 1
            else:
                if self.data.high >= self.data.high[-self.ref_max]:
                    msg += 'max incluso, '
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
                    msg += 'max NON incluso, TBD, '
                    self.ref_min += 1
                    self.ref_max += 1
            else:
                if self.data.low <= self.data.low[-self.ref_min]:
                    msg += 'min incluso, '
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


    def ex_outside(self, msg):
        msg += 'outside, '
        if self.ref_max > self.ref_min:
            if self.data.high >= self.data.high[-self.ref_max]:
                msg += 'max incluso, '
                self.ref_max = 1 
            else:
                msg += 'max NON incluso, '
                self.ref_max += 1

        elif self.ref_max < self.ref_min:
            if self.data.low <= self.data.low[-self.ref_min]:
                msg += 'min incluso, '
                self.ref_min = 1 
            else:
                msg += 'min NON incluso, '
                self.ref_min += 1

        else: #superfluo
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

    def next(self):

        msg = ''
        #self.log.info(self.data.datetime.datetime().strftime('%d-%m-%Y')+ ' low: ' + repr(self.data.low[0]) + ', high:' + repr(self.data.high[0]))

        if not self._inside(self.lookback):
            if not self._outside(self.lookback): 
                if self._up(self.lookback):  
                    msg += 'UP, '
                    if self.prev < 0: # down
                        msg += 'prev=down, '
                        if self.ref_max != 0: 
                            self.LW_max[-self.ref_max] = self.data.high[-self.ref_max] 
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

                elif self._down(self.lookback): # 3^ 
                    msg += 'DOWN, '
                    if self.prev > 0: #up
                        msg += 'prev=up, '
                        if (self.ref_min != 0): 
                            self.LW_min[-self.ref_min] = self.data.low[-self.ref_min]
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
                msg += self._outside_01(msg)
                #msg += self.ex_outside(msg)

            self.lookback = 1

        else: # inside
            self.lookback += 1
            msg += 'inside, '
            if self.ref_min != 0: self.ref_min += 1
            if self.ref_max != 0: self.ref_max += 1
        
        msg += ' ref_min ='+str(self.ref_min)+', ref_max='+str(self.ref_max) + ', prev='+str(self.prev)

        ### LAST Datapoint
        #
        if len(self.data) == self.data.buflen():
            msg += ', LAST '
            if self.ref_max > self.ref_min: self.LW_max[-self.ref_max+1] = self.data.high[-self.ref_max+1]
            elif self.ref_max < self.ref_min: self.LW_min[-self.ref_min+1] = self.data.low[-self.ref_min+1]

        self.log.info(self.data.datetime.datetime().strftime('%d-%m-%Y')+ ' - ' + msg + ', lookback='+str(self.lookback))
