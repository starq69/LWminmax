#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
backtrader strategy test main module (run.py)
'''
from datetime import datetime
import os, sys
from loader import load_adapter
import logging, logging.config, configparser
from I_LWminmaxIndicator import LWminmaxIndicator as LWminmax  
from I_InsideIndicator import InsideIndicator
import backtrader as bt
import backtrader.feeds as btfeeds
import pandas as pd


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
        print('################# ' + str(type(app_config)))

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

    #_indicators = dict()    # # #

    _name = 'STRATEGY' # corrisponde alla sezione in app.ini

    def __init__(self, config=None):

        self.log = logging.getLogger (__name__)
        self.log.info('ENTER STRATEGY '+repr(self.__class__))

        if config is not None and isinstance(config, configparser.ConfigParser):
            IND = [_ind.strip() for _ind in config.get(MyStrategy._name, 'ind').split(',')]
        else:
            print('invalid **kwarg param \'config\' passed to MyStrategy instance')
            sys.exit(1)

        self.loop_count = 0
        
        self.indicators     = dict() ##

        for i_name in IND:
            print(i_name)
            self.indicators[i_name] = dict()
            _mod = self.indicators[i_name]['__module__'] = load_adapter('_unused_', i_name) # .. può restituire direttamente l'istanza dell'indicatore?
            #_ind = _mod.init(*args, **kwargs) ...in questo caso doto ogni modulo di indicatori di una init() che restituisce un'istanza dell'indicatore

        for k, v in self.indicators.items():
            print(str(type(v)))


        I_LWminmax          = self.indicators[LWminmax._name]         = dict()    ##
        I_InsideIndicator   = self.indicators[InsideIndicator._name]  = dict()    ##
        
        #I_LWminmax          = MyStrategy._indicators[LWminmax._name]    = dict()    # # #
        #I_InsideIndicator   = MyStrategy._indicators[InsideIndicator._name] = dict()

        for _, datafeed in enumerate(self.datas):
            self.log.info('*** datafeed name : ' + datafeed._name) ## https://www.backtrader.com/blog/posts/2017-04-09-multi-example/multi-example.html
            df_name = datafeed._name
            
            I_LWminmax[df_name] = dict()
            I_LWminmax[df_name]['backtrader_indicator']         = LWminmax(datafeed, strategy=self)
            I_LWminmax[df_name]['backtrader_indicator'].csv     = True
            I_LWminmax[df_name]['output_dataframe']             = pd.DataFrame()

            I_InsideIndicator[df_name] = dict()
            I_InsideIndicator[df_name]['backtrader_indicator']      = InsideIndicator(datafeed, strategy=self)
            I_InsideIndicator[df_name]['backtrader_indicator'].csv  = True
            I_InsideIndicator[df_name]['output_dataframe']          = pd.DataFrame()
            

    def next_report(self):
        '''
        for _, datafeed in enumerate(d for d in self.datas if len(d)):
            msg = ''
            msg += datafeed.datetime.datetime().strftime('%d-%m-%Y') + ' <' + datafeed._name + '>' 
            _max = self.lw_min_max[datafeed._name].lines.LW_max[0]
            #_max = self.indicators[LWminmax._name][datafeed._name]['backtrader_indicator'].lines.LW_max[0]
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

        self.log.info('EXIT STRATEGY '+repr(self.__class__) + ', strategy.next loop_count = ' + str(self.loop_count))

        for indicator, obj in self.indicators.items():
        #for indicator, obj in MyStrategy._indicators.items():
            for datafeed, i_dict in obj.items():
                print(str(indicator) + '/' + str(datafeed))
                print(i_dict['output_dataframe'])               ### NEW


def main():

    log, app_config = setting_up()

    path = app_config['DATASOURCE']['path']

    ''' se aggiungo nell'app_config gli indicatori debbo passare app_config quando chiamo cerebro.addstrategy(MyStategy) '''

    cerebro = bt.Cerebro(stdstats=False) ###

    for (name, fname) in app_config['DATAFEEDS'].items():
        cerebro.adddata(btfeeds.YahooFinanceCSVData(dataname=path+fname, adjclose=False, decimals=5), name)
        log.info('Configured DATAFEED : ' + name +'-->'+fname + ' Succesfully added to cerebro')

    cerebro.addwriter(bt.WriterFile, csv=True, out="output.csv")
    #cerebro.addstrategy(MyStrategy)
    cerebro.addstrategy(MyStrategy, config=app_config)

    cerebro.broker.setcash(10000.0)
    cerebro.run()    
    #cerebro.plot(style='candlestick', barup='green', bardown='black')
    log.info('*** finished ***')


if __name__ == '__main__':
    main()
