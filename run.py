#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
backtrader strategy test main module (run.py)
'''
from datetime import datetime
import os, sys
import logging, logging.config, configparser
from I_LWminmaxIndicator import LWminmaxIndicator as LWminmax  
import backtrader as bt
import backtrader.feeds as btfeeds

def isNaN(num):
    return num != num

def setting_up():

    base_dir    = os.path.dirname (os.path.realpath(__file__))
    parent_dir  = os.path.split (base_dir)[0]
    cfg_file    = parent_dir + '/app.ini'
    cfg_log     = parent_dir + '/log.ini'

    try:
        logging.config.fileConfig (cfg_log)
        log = logging.getLogger (__name__)

    except Exception as e:
        print ('EXCEPTION during logging setup -> system stopped : {}'.format(e))
        sys.exit(1)

    try:
        app_config = configparser.ConfigParser() #allow_no_value=True)

        if not app_config.read (cfg_file):          ### Return list of successfully read files
            log.error('missing app configuration file <{}> : ABORT....'.format(cfg_file))
            sys.exit(1)

        log.info('***********************')
        log.info('*** session started ***')
        log.info('*** session configuration file <{}> loaded'.format(cfg_file))
        log.info('***********************')

    except configparser.Error as e:
        log.error ('INTERNAL ERROR : {}'.format (e))
        log.error ('ABORT')
        sys.exit(1)

    return log, app_config


class MyStrategy(bt.Strategy):

    def __init__(self):
        self.log = logging.getLogger (__name__)
        self.log.info('ENTER STRATEGY '+repr(self.__class__))

        self.loop_count = 0

        # calcola LWminmaxIndicator x tutti i datafeeds
        #
        self.lw_min_max = dict()
        for _, datafeed in enumerate(self.datas):
            self.log.info('*** datafeed name : ' + datafeed._name)
            self.lw_min_max[datafeed._name] = LWminmax(datafeed)

    def next_report(self):

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


    def next(self):
        ### print max/min for log analisys purpose
        #
        self.loop_count += 1
        self.next_report()

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

        self.log.info('EXIT STRATEGY '+repr(self.__class__) + ', strategy.next loop_count = ' + str(self.loop_count))


def main():

    log, app_config = setting_up()

    path = app_config['DATASOURCE']['path']

    cerebro = bt.Cerebro(stdstats=False) ###

    for (name, fname) in app_config['DATAFEEDS'].items():
        cerebro.adddata(btfeeds.YahooFinanceCSVData(dataname=path+fname, adjclose=False, decimals=5), name)
        log.info('Configured DATAFEED : ' + name +'-->'+fname + ' Succesfully added to cerebro')

    cerebro.addstrategy(MyStrategy)

    cerebro.broker.setcash(10000.0)
    cerebro.run()    
    cerebro.plot(style='candlestick', barup='green', bardown='black')

    log.info('*** finished ***')

if __name__ == '__main__':
    main()
