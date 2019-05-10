#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
backtrader strategy test main module (run.py)
'''
from datetime import datetime
import os, sys
from loader import load_module
import logging, logging.config, configparser
import backtrader as bt
import backtrader.feeds as btfeeds
import pandas as pd


def isNaN(num):
    return num != num


def get_strategy_class():
    return S_Datapoint_Analisys


class S_Datapoint_Analisys(bt.Strategy):

    def __init__(self, config=None, name=None):

        self.name = name

        self.log = logging.getLogger (__name__)
        self.log.info('ENTER STRATEGY <' + self.name + '> ' + repr(self.__class__))

        if config is not None and isinstance(config, configparser.ConfigParser):
            try:
                configured_indicators = [_ind.strip() for _ind in config.get('STRATEGIES', name).split(',') if len(_ind)]
            except configparser.NoOptionError as e:
                print('error : {}'.format(e))
                sys.exit(1)
        else:
            print('invalid **kwarg params passed to <' + repr(self.__class__) + '> instance')
            sys.exit(1)

        self.loop_count = 0
        
        self.indicators     = dict() ##

        for i_name in configured_indicators:

            self.indicators[i_name] = dict()
            _mod        = self.indicators[i_name]['__module__'] = load_module(i_name) # .. può restituire direttamente l'istanza dell'indicatore?
            ind_class   = self.indicators[i_name]['__class__']  = _mod.get_indicator_class()

            for _, datafeed in enumerate(self.datas):
                self.indicators[i_name][datafeed._name] = dict()
                self.indicators[i_name][datafeed._name]['indicator_instance'] = ind_class(datafeed, strategy=self)
                self.indicators[i_name][datafeed._name]['output_dataframe']   = pd.DataFrame()
            

    def next_report(self):
        '''
        for _, datafeed in enumerate(d for d in self.datas if len(d)):
            msg = ''
            msg += datafeed.datetime.datetime().strftime('%d-%m-%Y') + ' <' + datafeed._name + '>' 
            _max = self.lw_min_max[datafeed._name].lines.LW_max[0]
            if not isNaN(_max):
                msg += ', MAX : ' + str(_max)
            _min = self.lw_min_max[datafeed._name].lines.LW_min[0]    
            if not isNaN(_min):
                msg += ', MIN : ' + str(_min)

            _min_inter = self.lw_min_max[datafeed._name].lines.LW_min_inter[0]    
            if not isNaN(_min_inter):
                msg += ', MIN-INTER : ' + str(_min_inter)

            _max_inter = self.lw_min_max[datafeed._name].lines.LW_max_inter[0]    
            if not isNaN(_max_inter):
                msg += ', MAX-INTER : ' + str(_max_inter)

            _inside = self.lw_min_max[datafeed._name].lines.inside[0]
            if not isNaN(_inside):
                msg += ', inside = ' + str(int(_inside)) 

            self.log.info(msg)
        '''
        pass

    def next(self):
        ### print max/min for log analisys purpose
        #
        self.loop_count += 1
        #self.next_report()

        '''
        # https://community.backtrader.com/topic/187/multiple-symbols-each-symbol-multiple-time-frames-issue/7
        print(','.join(str(x) for x in [
            'Strategy', len(self),
            self.datetime.datetime().strftime('%Y-%m-%dT%H:%M:%S')])
        )

        for i, d in enumerate(d for d in self.datas if len(d)):
            out = ['Data' + str(i), len(d),
                   d.datetime.datetime().strftime('%Y-%m-%dT%H:%M:%S'),
                   d.open[0], d.high[0], d.low[0], d.close[0]]
            print(', '.join(str(x) for x in out))
        '''

    def stop(self):
        '''
        https://community.backtrader.com/topic/1448/convert-datas-0-into-a-pandas-dataframe/2
        https://community.backtrader.com/topic/11/porting-a-pandas-dataframe-dependent-indicator/2
        ...As for writing the values to a DataFrame, you may pass a DataFrame as a named argument to the indicator and add the values, 
        but taking into account that appending values to a DataFrame is a very expensive operation, you may prefer to do it at once during Strategy.stop
        '''

        self.log.info('EXIT STRATEGY <' + self.name + '> ' + repr(self.__class__) + ', strategy.next loop_count = ' + str(self.loop_count))

        for indicator, _dict in self.indicators.items():
            for item, detail in _dict.items():

                print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                try:
                    print(detail['output_dataframe'])
                    print(item)
                except TypeError as e:
                    print('skipped item') 
                    pass
                print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
