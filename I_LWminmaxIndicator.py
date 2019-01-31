#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import (absolute_import, division, print_function, unicode_literals)
import logging
#from I_InsideIndicator import InsideIndicator, New_InsideIndicator
from I_OutsideIndicator import OutsideIndicator, ex_OutsideIndicator
import backtrader as bt
import backtrader.indicators as btind

def isNaN(num):
    return num != num


class LWminmaxIndicator(bt.Indicator):

    lines       =('LW_max', 'LW_min', 'LW_max_inter', 'LW_min_inter', 'inside')
    #params      = dict(inside=New_InsideIndicator, outside=OutsideIndicator)
    #params = (('lookback',1),)
    plotinfo    = dict(subplot=False)
    plotlines   = dict(
        LW_max=dict(marker='^', markersize=8.0, color='green', fillstyle='full'),
        LW_min=dict(marker='v', markersize=8.0, color='red', fillstyle='full'),
        LW_max_inter=dict(marker='<', markersize=12.0, color='blue', fillstyle='full'),
        LW_min_inter=dict(marker='<', markersize=12.0, color='black', fillstyle='full'),

    )

    def __init__(self):
        self.log = logging.getLogger (__name__)
        self.prev = self.min_max_inter_flag = 0
        self.ref_min = self.ref_max = self.lookback = 1
        self.neg_data_idx = 0
        self.ref_max_list = [] # index list
        self.ref_min_list = []
        self.test_max = []
        self.test_min = []

        # da rimuovere (per ora serve ad attivare prenext())
        self.down       = bt.And(self.data.low(0) < self.data.low(-self.lookback), self.data.high(0) <= self.data.high(-self.lookback))

        #super(LWminmaxIndicator, self).__init__()

    def prenext(self):
        self.log.info(self.data.datetime.datetime().strftime('%d-%m-%Y')+ ' PRENEXT --> low: ' + repr(self.data.low[0]) + ', high:' + repr(self.data.high[0]))
        self.neg_data_idx -= 1


    def _outside_01(self, msg):
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


    def reset_last_inter(self, LW_inter):
        #
        #
        for idx, value in reversed(list(enumerate(LW_inter))):
            if not isNaN(value):
                LW_inter[idx] = float("nan") 
                break


    def get_last_inter_value(self, LW_inter):
        q = 0
        for idx, value in reversed(list(enumerate(LW_inter))):
            q += 1
            #print('<<< ' + str(value) + '>>>')
            if not isNaN(value):
                return value
            #print(str(q) + ' <<<<<<<<<<<<<<<<<<<<<<<') 
        return None


    def update_offset_idx(self, reset=False):
        if not reset:
            if self.c_inter_max != 0: self.c_inter_max -= 1 
        else:
            self.c_inter_max = 0



    # usare collections.deque: https://stackoverflow.com/questions/4426663/how-to-remove-the-first-item-from-a-list
    #
    def check_intermediate_max(self, ref_max):

        self.test_max.append(self.LW_max[ref_max])
        msg = ''
        if len(self.test_max) == 3:
            if self.test_max[0] < self.test_max[1] and self.test_max[1] > self.test_max[2]:
                #
                # nel caso stia cercando un minimo...
                #
                msg += ', {'+str(self.min_max_inter_flag)+'}, '
                if self.min_max_inter_flag <= 0:

                    # questo blocco non và : cambiare approccio
                    # non è possibile manipolare gli item di lines già impostati quindi
                    # userò un candidato mediante un indice offset (da mantenere aggiornato)
                    # il candidato esistente verrà confrontato con quello appena riscontrato --> self.test_max[1]
                    # ed in base al confronto si aggiorna il nuovo candidato la cui conferma
                    # avviene poi sul metodo check_intermediate_min() quando cioè termina la
                    # sequenza di max intermedi consecutivi (unico scenario in cui si prevede
                    # il confronto tra 2 candidati)
                    #
                    #
                    last_max = self.get_last_inter_value(self.LW_max_inter)
                    msg += '<<' + str(last_max) + '>>' 
                    if last_max is not None and last_max < self.test_max[1]:
                        self.log.info('>>> RESET max')
                        self.reset_last_inter(self.LW_max_inter)
                    #
                    #

                self.LW_max_inter[ref_max] = self.test_max[1] 
                last_max = self.get_last_inter_value(self.LW_max_inter) ### test
                print('**************** ' + str(ref_max) + ' ***************')


                msg += ' INTER.MAX: ' + str(self.LW_max_inter[ref_max]) + ', '
                #msg += 'len(' + str(len(self.LW_max_inter)) + ')'
                #for idx in range(len(self.LW_min_inter)): 
                #    print(self.LW_min_inter[idx]) # tutti nan !!!
                for x in range(2): self.test_max.pop(0)
                self.min_max_inter_flag = -1 # ricerca minimo
            else:
                self.test_max.pop(0)
        return msg


    def check_intermediate_min(self, ref_min):

        self.test_min.append(self.LW_min[ref_min])
        msg = ''
        if len(self.test_min) == 3:
            if self.test_min[0] > self.test_min[1] and self.test_min[1] < self.test_min[2]:
                #
                #
                #
                msg += ', {'+str(self.min_max_inter_flag)+'}, '
                if self.min_max_inter_flag >= 0:
                    last_min = self.get_last_inter_value(self.LW_min_inter)
                    msg += '<<' + str(last_min) + '>>' 
                    if last_min is not None and last_min < self.test_min[1]:
                        self.log.info('>>>>>>>>>>>>>segue reset_last_inter(LW_min_inter)')
                        self.reset_last_inter(self.LW_min_inter)
                    
                self.LW_min_inter[ref_min] = self.test_min[1]
                msg += ', {'+str(self.min_max_inter_flag)+'} INTER.MIN: ' + str(self.LW_min_inter[ref_min]) + ', '
                for x in range(2): self.test_min.pop(0)
                self.min_max_inter_flag = 1 # ricerca massimo
            else:
                self.test_min.pop(0)
        return msg


    def next(self):

        msg = ''
        self.neg_data_idx -= 1
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

                elif self._down(self.lookback): # 3^ 
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
                msg += self._outside_01(msg)

            self.lookback = 1

        else: # inside
            self.lookback += 1
            msg += 'inside, '
            if self.ref_min != 0: self.ref_min += 1
            if self.ref_max != 0: self.ref_max += 1
        
        #self.l.inside[0] = self.lookback - 1
        if self.lookback > 1: self.l.inside[0] = self.lookback - 1
 

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

            #point = 2 ### point corrisp. a neg_data_idx in un det. momento
            #print('****** ------>' + str(self.data.high[-(self.neg_data_idx - point)]))



        self.log.info(self.data.datetime.datetime().strftime('%d-%m-%Y')+ ' - ' + msg + ', lookback='+str(self.lookback))
