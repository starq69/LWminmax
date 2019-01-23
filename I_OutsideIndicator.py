#!/usr/bin/env python
# -*- coding: utf-8 -*-

import backtrader as bt
import backtrader.indicators as btind

class OutsideIndicator(bt.Indicator):
    lines = ('outside',)

    def __init__(self):

        self.outside_high = self.data.high > self.data.high(-1)
        self.outside_low  = self.data.low  < self.data.low(-1)

        super(OutsideIndicator, self).__init__()
    
    def next(self):

        if self.outside_high and self.outside_low:
            self.outside[0] = 10
        else:
            self.outside[0] = 0


class ex_OutsideIndicator(bt.Indicator):
    lines = ('outside',)
    params = (('ref', 1),)

    def __init__(self):

        self.outside_high = self.data.high(0) > self.data.high(-int(self.p.ref))
        self.outside_low  = self.data.low(0)  < self.data.low(-int(self.p.ref))
        super(ex_OutsideIndicator, self).__init__()
    
    def next(self):

        #self.outside_high = self.data.high[0] > self.data.high[-int(self.p.ref)]
        #self.outside_low  = self.data.low[0]  < self.data.low[-int(self.p.ref)]

        #if self.outside_high and self.outside_low:

        self.outside = bt.If(self.outside_high and self.outside_low, 1, 0)

        #if bt.And(self.outside_high, self.outside_low):
        #    self.outside[0] = 1
        #else:
        #    self.outside[0] = 0

