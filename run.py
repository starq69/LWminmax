#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, re
import subprocess
import datetime
from loader import load_module
import logging, logging.config, configparser
import backtrader as bt
import backtrader.feeds as btfeeds
from syncdb import syncdb


class NoStrategyFound(Exception):
    pass


class NoSecurityFound(Exception):
    pass


def remove_postfix(s):
    try:
        _id = re.match(r"(.*)_\d+$", s).group(1)
    except AttributeError as e:
        _id = s

    return _id


def setting_up():

    base_dir    = os.path.dirname (os.path.realpath(__file__))
    parent_dir  = os.path.split (base_dir)[0]
    cfg_file    = parent_dir + '/app.ini'
    cfg_log     = parent_dir + '/log.ini'
    syncdb_dir  = parent_dir + '/local_storage/'
    syncdb_file = 'syncdb_default.db'

    try:
        logging.config.fileConfig (cfg_log)
        log = logging.getLogger (__name__)

    except Exception as e:
        log.exception('EXCEPTION during logging setup -> system stopped : {}'.format(e))
        sys.exit(1)

    try:
        app_config = configparser.ConfigParser() #allow_no_value=True)
        app_config.optionxform = str    # non converte i nomi opzione in lowercase (https://docs.python.org/2/library/configparser.html#ConfigParser.RawConfigParser.optionxform)

        if not app_config.read (cfg_file):
            log.error('missing app configuration file <{}> : ABORT....'.format(cfg_file))
            sys.exit(1)

        log.info('*** SESSION STARTED ***')
        log.info('configuration file <{}> LOADED'.format(cfg_file))

        syncdb_instance = syncdb(db_dir=syncdb_dir ,db_file=syncdb_file) 

    except configparser.Error as e:
        log.error ('INTERNAL ERROR : <{}>'.format (e))
        sys.exit(1)
        #raise e !!

    except Exception as e:
        log.error ('INTERNAL ERROR : <{}>'.format (e))
        sys.exit(1) 
        #raise e !!

    return log, app_config, syncdb_instance


def import_strategies(app_config):

    log = logging.getLogger (__name__)
    strategy_modules = dict()
    strategy_classes = dict()

    try:
        strategies = [ss for ss in app_config.options('STRATEGIES') if len(ss)]
    except configparser.Error as e:
        raise e

    else:
        if not strategies:
            raise NoStrategyFound('No strategy found on configuration, pls specify at list one in section STRATEGIES')
        else:
            for strategy in strategies:
                strategy_id = remove_postfix(strategy)
                try:
                    if strategy_id not in strategy_modules:
                        strategy_modules[strategy_id] = load_module(strategy_id) 
                        strategy_classes[strategy_id] = strategy_modules[strategy_id].get_strategy_class()
                        log.info(str(strategy_modules[strategy_id]) + ' for strategy <' + strategy + '> succesfully added to cerebro')
                    else:
                        # TEST : strategy_classes che valore ha qui ?
                        log.info(str(strategy_modules[strategy_id]) + ' for strategy <' + strategy + '> already loaded')
                except Exception as e:
                    raise e

    return strategies, strategy_classes


def check_securities(app_config, syncdb):

    try:
        securities = [ss.strip() for ss in app_config.get('DATAFEEDS', 'securities').split(',') if len(ss)]
    except configparser.NoOptionError as e:
        raise e
    if not len(securities):
        raise NoSecurityFound('No securities found on configuration!')

    # select from syncdb.securities
    #
    return syncdb.load_securities(securities) 

'''
def update_security_cache(security_id, fromdate, todate, syncdb):
    syncdb.insert_security(security_id, fromdate, todate)
'''

def main():

    log, app_config, syncdb = setting_up()

    path            = app_config['DATASOURCE']['path']
    cerebro         = bt.Cerebro(stdstats=False) 

    try: # estendere fino in fondo

        strategies, strategy_classes = import_strategies (app_config)

        securities = check_securities (app_config, syncdb)

        # load_datafeeds(securities)
        #
        for security_id, _struct in securities.items():
            default_fromdate = '2018-01-01' # da config.
            default_todate   = '2018-12-31' # ...magari = oggi ?
            datafile         = '../local_storage/yahoo_csv_cache/'+security_id+'.csv'   #+#

            if _struct is None or not os.path.isfile(datafile):
                #
                # update syncdb/storage
                #
                syncdb.insert_security(security_id, default_fromdate, default_todate, datafile=datafile)
                
            data = btfeeds.YahooFinanceCSVData (dataname=datafile,    #+#
                                                    fromdate=datetime.datetime(2016, 1, 1),
                                                    todate=datetime.datetime(2018, 12, 31),
                                                    adjclose=False, 
                                                    decimals=5)

            cerebro.adddata(data, security_id)    
            log.info('datafeed <' + security_id +'> succesfully added to cerebro')


        #cerebro.addwriter(bt.WriterFile, csv=True, out="output.csv")
        cerebro.broker.setcash(10000.0)
        
        for strategy in strategies:
            strategy_id = remove_postfix(strategy)
            cerebro.addstrategy(strategy_classes[strategy_id], config=app_config, name=strategy)
            #cerebro.run()    
            #cerebro.plot(style='candlestick', barup='green', bardown='black')

        cerebro.run()
        syncdb.close()
        log.info('*** finished ***')

    except Exception as e: #BacktraderError as e ?
        log.exception(e)
        try:
            syncdb.close()
        except Exception:
            pass
        sys.exit(1)


if __name__ == '__main__':
    main()
