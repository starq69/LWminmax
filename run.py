#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, re
from loader import load_module
import logging, logging.config, configparser
import backtrader as bt
import backtrader.feeds as btfeeds


class NoStrategyFound(Exception):
    pass


class NoSecurityFound(Exception):
    pass


def isNaN(num):
    ''' spostare '''
    return num != num


def remove_postfix(s):
    try:
        _s = re.match(r"(.*)_\d+$", s).group(1)
    except AttributeError as e:
        _s = s

    return _s


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
        app_config.optionxform = str    # non converte i nomi opzione in lowercase (https://docs.python.org/2/library/configparser.html#ConfigParser.RawConfigParser.optionxform)

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


def import_strategies(app_config):

    log = logging.getLogger (__name__)
    strategy_modules = dict()

    try:
        strategies = [ss for ss in app_config.options('STRATEGIES') if len(ss)]
    except configparser.Error as e:
        #log.error ('During load strategies from config file : {}'.format (e))
        #sys.exit(1)
        raise e

    else:
        if not strategies:
            raise NoStrategyFound('No strategy found on configuration, pls specify at list one in section STRATEGIES')
        else:
            for strategy in strategies:
                strategy_fixed = remove_postfix(strategy)
                try:
                    if strategy_fixed not in strategy_modules:
                        strategy_modules[strategy_fixed] = load_module(strategy_fixed) 
                except Exception as e:
                    log.error('Exception : {}'.format(e))
                    sys.exit()
                    #raise e
                else:
                    log.info(str(strategy_modules[strategy_fixed]) + ' for strategy <' + strategy + '> succesfully added to cerebro')

    return strategies, strategy_modules


def check_securities(app_config):

    try:
        securities = [_code.strip() for _code in app_config.get('SECURITIES', 'codes').split(',') if len(_code)]
    except configparser.NoOptionError as e:
        raise e
    if not len(securities):
        raise NoSecurityFound('No securities found on configuration!')

    return securities


def main():

    log, app_config = setting_up()
    path            = app_config['DATASOURCE']['path']
    cerebro         = bt.Cerebro(stdstats=False) 

    #strategy_modules = dict()

    try:
        strategies, strategy_modules = import_strategies (app_config)

        securities = check_securities (app_config)

        for security in securities:
            cerebro.adddata (btfeeds.YahooFinanceCSVData(dataname=path+security+'.csv', adjclose=False, decimals=5), security)
            log.info('Configured DATAFEED : ' + security +'-->' + security + '.csv  Succesfully added to cerebro')

    except Exception as e:
        log.exception(e)
        sys.exit(1)



    #cerebro.addwriter(bt.WriterFile, csv=True, out="output.csv")
    cerebro.broker.setcash(10000.0)
    
    for strategy in strategies:
        strategy_fixed = remove_postfix(strategy)
        #cerebro.addstrategy(st.get_strategy_class(), config=app_config)
        cerebro.addstrategy(strategy_modules[strategy_fixed].get_strategy_class(), config=app_config, name=strategy)
        #cerebro.run()    
        #cerebro.plot(style='candlestick', barup='green', bardown='black')

    cerebro.run()
    log.info('*** finished ***')


if __name__ == '__main__':
    main()
