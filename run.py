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
            try:
                configured_indicators = [_ind.strip() for _ind in config.get(MyStrategy._name, 'ind').split(',') if len(_ind)]
            except configparser.NoOptionError as e:
                print('malformed configuration file : add option \'ind\' to section ' + MyStrategy._name)
                sys.exit(1)
        else:
            print('invalid **kwarg param \'config\' passed to MyStrategy instance')
            sys.exit(1)

        self.loop_count = 0
        
        self.indicators     = dict() ##

        for i_name in configured_indicators:

            self.indicators[i_name] = dict()
            _mod        = self.indicators[i_name]['__module__'] = load_adapter('_unused_', i_name) # .. può restituire direttamente l'istanza dell'indicatore?
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

        self.log.info('EXIT STRATEGY '+repr(self.__class__) + ', strategy.next loop_count = ' + str(self.loop_count))

        for indicator, _dict in self.indicators.items():
            for item, detail in _dict.items():

                print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

                try:
                    print(detail['output_dataframe'])
                    print(item)
                except TypeError as e:
                    print('skipped item') 
                    pass
                '''
                if type(detail) is dict:
                    print('key : ' + item)
                    print(detail)
                else:
                    print(str(indicator) + '/' + str(item))
                '''
                print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')


def main():

    log, app_config = setting_up()

    path = app_config['DATASOURCE']['path']

    cerebro = bt.Cerebro(stdstats=False) ###

    try:
        securities = [_code.strip() for _code in app_config.get('SECURITIES', 'codes').split(',') if len(_code)]
    except configparser.NoOptionError as e:
        print('malformed configuration file : add option \'codes\' to section SECURITIES')
        sys.exit(1)

    for security in securities:
        cerebro.adddata(btfeeds.YahooFinanceCSVData(dataname=path+security+'.csv', adjclose=False, decimals=5), security)
        log.info('Configured DATAFEED : ' + security +'-->' + security + '.csv  Succesfully added to cerebro')

    cerebro.addwriter(bt.WriterFile, csv=True, out="output.csv")

    cerebro.addstrategy(MyStrategy, config=app_config)

    cerebro.broker.setcash(10000.0)
    cerebro.run()    
    #cerebro.plot(style='candlestick', barup='green', bardown='black')
    log.info('*** finished ***')


if __name__ == '__main__':
    main()
