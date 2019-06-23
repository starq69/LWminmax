#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, re
import logging, logging.config, configparser
#import argparse
import datetime as dt
import subprocess
from loader import load_module
import backtrader as bt
import backtrader.feeds as btfeeds
from syncdb import syncdb
from settings import * 


class NoStrategyFound(Exception):
    pass


class NoSecurityFound(Exception):
    pass


def as_dict(config):
    """
    https://stackoverflow.com/questions/1773793/convert-configparser-items-to-dictionary/23944270#23944270
    Converts a ConfigParser object into a dictionary.

    The resulting dictionary has sections as keys which point to a dict of the
    sections options as key => value pairs.
    """
    _dict = {}
    for section in config.sections():
        _dict[section] = {}
        for key, val in config.items(section):
            _dict[section][key] = val
    return _dict


def remove_postfix(s):
    try:
        _id = re.match(r"(.*)_\d+$", s).group(1)
    except AttributeError as e:
        _id = s
    return _id


def import_strategies(app_config):

    log = logging.getLogger (__name__)
    strategy_modules = dict()
    strategy_classes = dict()

    try:
        strategies = [ss for ss in app_config['STRATEGIES'].keys() if len(ss)]
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
                        log.debug('module {} for strategy <{}> succesfully added to cerebro'.format(str(strategy_modules[strategy_id]),strategy))
                    else:
                        # TODO TEST : strategy_classes che valore ha qui ?
                        log.debug('module {} for strategy <{}> already loaded'.format(str(strategy_modules[strategy_id]),strategy))
                except Exception as e:
                    raise e
    return strategies, strategy_classes


def load_securities(app_config, syncdb):
    '''
    try:
        # TODO
        #securities = [ss.strip() for ss in app_config.get('DATAFEEDS', 'securities').split(',') if len(ss)]
        securities = [ss.strip() for ss in app_config['DATAFEEDS']['securities'] if len(ss)]
    except configparser.NoOptionError as e:
        raise e # TODO TESTARE
    if not len(securities):
        raise NoSecurityFound('Empty security list found on configuration!')
    '''
    securities = [ss.strip() for ss in app_config['DATAFEEDS']['securities'] if len(ss)]
    if not len(securities):
        raise NoSecurityFound('Empty security list found on configuration!')

    return syncdb.load_securities (securities) 


def setting_up():

    def get_log (cfg_log):
        try:
            logging.config.fileConfig (cfg_log)
            log = logging.getLogger (__name__)
        except KeyError as e:
            print('EXCEPTION during logging setup on key {}'.format(e)) # TODO
            sys.exit(1)
        except (configparser.DuplicateOptionError, Exception) as e:
            print('EXCEPTION during logging setup : {}'.format(e))      # TODO
            sys.exit(1)
        else:
            log.info('*** BEGIN SESSION ***')
        return log


    def get_config (cfg_file):
        try:
            app_config = configparser.ConfigParser() #allow_no_value=True)
            app_config.optionxform = str # non converte i nomi opzione in lowercase

            if not app_config.read (cfg_file):
                log.error('MISSING configuration file <{}>'.format(cfg_file))
                log.info('Use default settings')
            else:
                log.info('configuration file <{}> LOADED'.format(cfg_file))
        except configparser.Error as e:
            log.error('ERROR during parsing configuration : <{}>'.format (e))
            log.info('UNABLE to load configuration file : we use default settings')
        except Exception as e:
            log.error('EXCEPTION during parsing configuration : <{}>'.format (e))
            log.info('UNABLE to load configuration file : we use default settings')

        return override_defaults([app_config, args_parser()])


    def get_syncdb (app_config):
        # TODO passare tutti i parametri di config. relativi a syncdb (non solo strict) e concatenare ev. version nel nome file db
        syncdb_dir  = app_config['STORAGE']['syncdb']
        path        = app_config['STORAGE']['yahoo_csv_data']
        if not syncdb_dir : syncdb_dir  = parent_dir + '/local_storage/'
        if not path       : path = parent_dir
        syncdb_file = 'syncdb_test.db' # TODO
        return syncdb (db_dir=syncdb_dir ,db_file=syncdb_file, path=path, strict=strict_mode(app_config))


    def strict_mode (app_config):
        _strict = app_config['OPTIONS']['strict'].strip().lower() # strip() non necessario ?
        if _strict in ['1', 'yes', 'true', 'on']:
            return True
        elif _strict in ['0', 'no', 'false', 'off'] :
            return False
        else: # ridondante ?
            log.warning('invalid strict mode : <{}> Now is set to False'.format(_strict))
            return False 


    args = vars(args_parser())

    if _log_settings_file_ in args:
        cfg_log = args[_log_settings_file_]
    else:
        cfg_log = _log_settings_file_name_

    if _ini_settings_file_ in args:
        cfg_file = args[_ini_settings_file_]
    else:
        cfg_file = _ini_settings_file_name_

    log             = get_log (cfg_log)
    app_config      = get_config (cfg_file)
    syncdb_instance = get_syncdb (app_config)
    run_fromdate    = dt.datetime.strptime(app_config['SECURITIES']['fromdate'], '%Y-%m-%d').date() # TODO
    run_todate      = dt.datetime.strptime(app_config['SECURITIES']['todate'], '%Y-%m-%d').date()   # TODO

    return log, app_config, syncdb_instance, run_fromdate, run_todate

#OLD
def parse_args(pargs=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=(
            'Multiple Values and Brackets'
        )
    )
    '''
    parser.add_argument('--data0', default='../../datas/nvda-1999-2014.txt',
                        required=False, help='Data0 to read in')
    '''
    # Defaults for dates
    yesterday = dt.datetime.strftime(dt.date.today() - dt.timedelta(days=1),'%Y-%m-%d')

    parser.add_argument('--fromdate', default='2018-01-01', help='Date in YYYY-MM-DD format')
    parser.add_argument('--todate', default=yesterday, help='Date in YYYY-MM-DD format')
    parser.add_argument('--strict', default='yes', choices=['yes', 'no'], help='strict can be yes or no')

    '''
    parser.add_argument('--cerebro', required=False, default='',
                        metavar='kwargs', help='kwargs in key=value format')

    parser.add_argument('--broker', required=False, default='',
                        metavar='kwargs', help='kwargs in key=value format')

    parser.add_argument('--sizer', required=False, default='',
                        metavar='kwargs', help='kwargs in key=value format')

    parser.add_argument('--strat', required=False, default='',
                        metavar='kwargs', help='kwargs in key=value format')

    parser.add_argument('--plot', required=False, default='',
                        nargs='?', const='{}',
                        metavar='kwargs', help='kwargs in key=value format')
    '''
    _args   = vars(parser.parse_args())
    _from   = dt.datetime.strptime(_args['fromdate'], '%Y-%m-%d').date()
    _to     = dt.datetime.strptime(_args['todate'], '%Y-%m-%d').date()
    _strict = _args['strict']

    return _from, _to, _strict


def main():

    log, app_config, syncdb, run_fromdate, run_todate  = setting_up()  
    print(app_config)

    try:
        cerebro         = bt.Cerebro(stdstats=False) 

        strategies, \
        strategy_classes = import_strategies (app_config)
        securities       = load_securities (app_config, syncdb)
        found            = False

        log.info('Analisys period is {} - {}'.format(run_fromdate, run_todate))

        # load_datafeeds(securities)
        #
        for security_id, _struct in securities.items():
            datafile = syncdb.select_security_datafeed (_struct, security_id, run_fromdate, run_todate) 
            if datafile:    
                data = btfeeds.YahooFinanceCSVData (dataname=datafile,
                                                    fromdate=run_fromdate,
                                                    todate=run_todate + dt.timedelta(days=1), 
                                                    adjclose=False, 
                                                    decimals=5)

                cerebro.adddata(data, security_id)    
                log.info('datafeed <{}> succesfully added to cerebro'.format(security_id))
                found = True

        if found: 
            #cerebro.addwriter(bt.WriterFile, csv=True, out="output.csv")
            cerebro.broker.setcash(10000.0)
            
            for strategy in strategies:
                strategy_id = remove_postfix(strategy)
                cerebro.addstrategy(strategy_classes[strategy_id],
                                    config=app_config, 
                                    name=strategy, 
                                    fromdate=run_fromdate, 
                                    todate=run_todate)

            cerebro.run()

            #cerebro.plot(style='candlestick', barup='green', bardown='black')
            syncdb.close()
        else:
            log.warning('~ No security found ~')

        log.info('*** END SESSION ***')

    except Exception as e:
        log.error('Abnormal END : {}'.format(e))
        try:
            syncdb.close()
        except Exception:
            pass
        sys.exit(1)


if __name__ == '__main__':
    main()
