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

        # compute LWminmaxIndicator over all datafeeds
        #
        self.lw_min_max = dict()
        for _, datafeed in enumerate(self.datas):
            self.log.info('*** datafeed name : ' + datafeed._name)
            self.lw_min_max[datafeed._name] = LWminmax(datafeed)

    def next(self):
        pass
        '''
        ### print max/min for log analisys purpose
        #
        for _, d in enumerate(self.datas):
            msg = d.datetime.datetime().strftime('%d-%m-%Y') + ' <' + d._name + '>'
            if not isNaN(self.lw_min_max[d._name].lines.LW_max[0]):
                self.log.info(msg + ' MAX : ' + str(self.lw_min_max[d._name].lines.LW_max[0]))
            if not isNaN(self.lw_min_max[d._name].lines.LW_min[0]):
                self.log.info(msg + ' MIN : ' + str(self.lw_min_max[d._name].lines.LW_min[0]))
        '''

    def stop(self):

        self.log.info('EXIT STRATEGY '+repr(self.__class__))


def main():

    log, app_config = setting_up()

    path = app_config['DATASOURCE']['path']

    cerebro = bt.Cerebro(stdstats=False) ###
    cerebro.addstrategy(MyStrategy)

    for (name, fname) in app_config['DATAFEEDS'].items():
        cerebro.adddata(btfeeds.YahooFinanceCSVData(dataname=path+fname, adjclose=False, decimals=5), name)
        log.info('Configured DATAFEED : ' + name +'-->'+fname + ' Succesfully added to cerebro')

    cerebro.broker.setcash(10000.0)
    cerebro.run()    
    cerebro.plot(style='candlestick', barup='green', bardown='black')

    log.info('*** finished ***')

if __name__ == '__main__':
    main()
