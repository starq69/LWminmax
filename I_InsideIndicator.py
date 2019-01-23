#!/usr/bin/env python
# -*- coding: utf-8 -*-

import backtrader as bt
import backtrader.indicators as btind

class InsideIndicator(bt.Indicator):
    lines = ('inside',)

    def __init__(self):

        self.i = 1
        super(InsideIndicator, self).__init__()
    
    def next(self):

        inside_high       = self.data.high[0] < self.data.high[- int(self.i)] ### <
        inside_low        = self.data.low[0]  > self.data.low[- int(self.i)]  ### >

        if inside_high and inside_low:
            self.i +=1
        else:
            self.i = 1
        
        self.inside[0] = self.i - 1


class New_InsideIndicator(bt.Indicator):
    lines = ('inside',)

    def __init__(self):

        self.i = 1
        super(New_InsideIndicator, self).__init__()
    
    def next(self):

        inside_high       = self.data.high[0] <= self.data.high[- int(self.i)] ### <
        inside_low        = self.data.low[0]  >= self.data.low[- int(self.i)]  ### >

        if inside_high and inside_low:
            self.i +=1
        else:
            self.i = 1

        self.inside[0] = self.i - 1
