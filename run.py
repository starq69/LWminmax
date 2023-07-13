#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, re
import logging, logging.config, configparser
import datetime as dt
from collections import OrderedDict
import subprocess
from loader import load_module
import backtrader as bt
import backtrader.feeds as btfeeds
from syncdb import syncdb
from csv_cache import csv_cache
from duckdb_data import duckdb_data
from settings import * 


class NoStrategyFound(Exception):
    pass


class NoSecurityFound(Exception):
    pass

class InvalidPeriod(BaseException):
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


def import_strategies(app_config):

    def remove_postfix(s):
        try:
            _id = re.match(r"(.*)_\d+$", s).group(1)
        except AttributeError as e:
            _id = s
        return _id

    log = logging.getLogger (__name__)
    strategy_modules = dict()
    strategy_classes = OrderedDict()


    try:
        strategy_labels = [ss for ss in app_config[_STRATEGIES_].keys() if len(ss)]
    except configparser.Error as e:
        raise e
    else:
        if not strategy_labels:
            raise NoStrategyFound('No strategy found on configuration, pls specify at list one in section STRATEGIES')
        else:
            for strategy_label in strategy_labels:
                strategy_id = remove_postfix(strategy_label)
                try:
                    if strategy_id not in strategy_modules:
                        strategy_modules[strategy_id] = load_module(strategy_id) 
                        log.debug('module {} for strategy <{}> succesfully added to cerebro'.format(str(strategy_modules[strategy_id]),strategy_label))
                    else:
                        # TODO TEST : strategy_classes che valore ha qui ?
                        log.debug('module {} for strategy <{}> already loaded'.format(str(strategy_modules[strategy_id]),strategy_label))
                    
                    strategy_classes[strategy_label] = strategy_modules[strategy_id].get_strategy_class()

                except Exception as e:
                    raise e

    return strategy_classes


def load_securities(app_config, syncdb):

    securities = [ss.strip() for ss in app_config[_DATAFEEDS_]['securities'] if len(ss)] #starq@2023
    if not len(securities):
        raise NoSecurityFound('Empty security list found on configuration!')

    return syncdb.load_securities (securities) 
    #return securities


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
                log.warning('MISSING configuration file <{}>'.format(cfg_file))
                log.info('Use default settings')
            else:
                log.info('configuration file <{}> LOADED'.format(cfg_file))
        except configparser.Error as e:
            log.error('during parsing configuration file : <{}>'.format (e))
            raise e
        except Exception as e:
            log.error('EXCEPTION during parsing configuration : <{}>'.format (e))
            raise e

        return override_defaults([app_config, args_parser()])


    def get_syncdb (app_config):
        '''starq@2023:TODO
            [DATAFEEDS]
            	source=sqlite/duckdb/csv_cache

            gestire source :
                =sqlite --> ASIS (usa sqlite3)
                =duckdb --> nuova classe di interfacciamento x duckdb (x Nasdaq --> tabella MNQ.M1)
                =local  --> nuova classe senza backend sql solo per load_securities/select_security_datafeed che usa quello che trova su /local_storage/csv_cache/  
                            sempre in base a quanto specificato in [DATAFEEDS]securities e [SECURITIES] from/todate
        '''

        _source_ = app_config[_DATAFEEDS_]['source']
        #log.info('--> source is <{}>'.format(_source_)
        
        if _source_ == 'sqlite':
            # TODO passare tutti i parametri di config. relativi a syncdb (non solo strict) e concatenare ev. version nel nome file db
            syncdb_dir  = app_config[_STORAGE_]['syncdb']
            path        = app_config[_STORAGE_]['yahoo_csv_data']
            if not syncdb_dir : syncdb_dir  = parent_dir + '/local_storage/'
            if not path       : path = parent_dir
            syncdb_file = 'syncdb_test.db' # TODO
            return syncdb (db_dir=syncdb_dir ,db_file=syncdb_file, path=path, strict=strict_mode(app_config))
        
        elif _source_ == 'duckdb':
            syncdb_dir  = app_config[_STORAGE_]['duckdb_data']
            path        = app_config[_STORAGE_]['yahoo_csv_data']
            syncdb_file = '001.db' # TODO

            return duckdb_data(db_dir=syncdb_dir, db_file=syncdb_file, strict=strict_mode(app_config))
        
        elif _source_ == 'csv_cache':
            return csv_cache(strict=strict_mode(app_config))


    def strict_mode (app_config):
        _strict = app_config[_OPTIONS_]['strict'].strip().lower() # strip() non necessario ?
        if _strict in ['1', 'yes', 'true', 'on']:
            return True
        elif _strict in ['0', 'no', 'false', 'off'] :
            return False
        else: # ridondante ?
            log.warning('invalid strict mode : <{}> Now is set to False'.format(_strict))
            return False 


    _expection = None

    args = vars(args_parser())

    if _log_settings_file_ in args:
        cfg_log = args[_log_settings_file_]
    else:
        cfg_log = _default_log_settings_file_

    if _ini_settings_file_ in args:
        cfg_file = args[_ini_settings_file_]
    else:
        cfg_file = _default_ini_settings_file_

    log             = get_log (cfg_log)
    app_config      = get_config (cfg_file)
    syncdb_instance = get_syncdb (app_config)
    run_fromdate    = dt.datetime.strptime(app_config[_SECURITIES_]['fromdate'], '%Y-%m-%d').date() # TODO
    run_todate      = dt.datetime.strptime(app_config[_SECURITIES_]['todate'], '%Y-%m-%d').date()   # TODO

    if run_fromdate > run_todate :
        _expection = 'Periodo NON valido : verificare fromdate/todate.'

    return _expection, log, app_config, syncdb_instance, run_fromdate, run_todate


def main():

    failure, log, app_config, syncdb, run_fromdate, run_todate  = setting_up()

    if failure is not None:
        log.exception(failure)

    else:

        try:
            cerebro          = bt.Cerebro(stdstats=False) 

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

                for strategy_label in strategy_classes:
                    cerebro.addstrategy(strategy_classes[strategy_label],
                                        config=app_config, 
                                        name=strategy_label, 
                                        fromdate=run_fromdate, 
                                        todate=run_todate)

                cerebro.run()
                #cerebro.plot(style='candlestick', barup='green', bardown='black')
                syncdb.close()
            else:
                log.warning('~ No security found ~')

            log.info('*** END SESSION ***')

        except Exception as e:
            log.error('ABEND : {}'.format(e))
            try:
                syncdb.close()
            except Exception:
                pass
            sys.exit(1)


if __name__ == '__main__':
    main()
