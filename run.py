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

    def strict_mode(app_config):
        try:
            strict_mode = app_config.getboolean('OPTIONS', 'strict')
            log.info('strict mode is {}'.format(strict_mode))
            return strict_mode
        except Exception as e:
            log.warning('invalid strict mode specified : {} Now is set to False'.format(e))
            return False # 


    base_dir    = os.path.dirname (os.path.realpath(__file__))
    parent_dir  = os.path.split (base_dir)[0]
    cfg_file    = parent_dir + '/app.ini'
    cfg_log     = parent_dir + '/log.ini'

    #################
    # TODO : se corrisponde al par. path della syncdb.select_security_datafeed() e della syncdb.select_file()
    #        rimuovere il par. ed utilizzare self.syncdb_dir ....
    try:
        syncdb_dir = app_config.get('STORAGE', 'syncdb')
    except Exception as e:
        syncdb_dir  = parent_dir + '/local_storage/'
    #
    #################

    syncdb_file = 'syncdb_test.db' # TODO TODO TODO concatenare eventuale version

    try:
        logging.config.fileConfig (cfg_log)
        log = logging.getLogger (__name__)

    except KeyError as e:
        print('EXCEPTION during logging setup on key {}'.format(e))
        sys.exit(1)
    except configparser.DuplicateOptionError as e:
        print('EXCEPTION during logging setup : {}'.format(e))
        sys.exit(1)

    try:
        app_config = configparser.ConfigParser() #allow_no_value=True)
        app_config.optionxform = str    # non converte i nomi opzione in lowercase (https://docs.python.org/2/library/configparser.html#ConfigParser.RawConfigParser.optionxform)

        if not app_config.read (cfg_file):
            log.exception('missing app configuration file <{}> : ABORT....'.format(cfg_file))
            sys.exit(1)

        log.info('*** BEGIN SESSION ***')
        log.info('configuration file <{}> LOADED'.format(cfg_file))

        strict = strict_mode(app_config)

        # asked_todate TODO aggiungere qui ?

        syncdb_instance = syncdb(db_dir=syncdb_dir ,db_file=syncdb_file, strict=strict) 

    except configparser.Error as e:
        log.error('INTERNAL ERROR : <{}>'.format (e))
        #sys.exit(1)
        raise e 

    except Exception as e:
        log.error('INTERNAL ERROR : <{}>'.format (e))
        #sys.exit(1) 
        raise e

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
                        log.debug('module {} for strategy <{}> succesfully added to cerebro'.format(str(strategy_modules[strategy_id]),strategy))
                    else:
                        # TODO TEST : strategy_classes che valore ha qui ?
                        log.debug('module {} for strategy <{}> already loaded'.format(str(strategy_modules[strategy_id]),strategy))
                except Exception as e:
                    raise e

    return strategies, strategy_classes


def load_securities(app_config, syncdb):

    try:
        securities = [ss.strip() for ss in app_config.get('DATAFEEDS', 'securities').split(',') if len(ss)]
    except configparser.NoOptionError as e:
        raise e
    if not len(securities):
        raise NoSecurityFound('Empty security list found on configuration!')

    # select from syncdb.securities
    #
    return syncdb.load_securities(securities) 


def main():

    log, app_config, syncdb = setting_up()

    today           = datetime.date.today() 
    asked_todate    = today - datetime.timedelta(days=1) # se nn specificato in config. TODO
    default_fromdate = '2018-06-01' # da config. # TODO
    path            = app_config['DATASOURCE']['path']
    cerebro         = bt.Cerebro(stdstats=False) 
    log.info('Analisys period is {} - {}'.format(default_fromdate, asked_todate))

    try: 
        strategies, \
        strategy_classes = import_strategies (app_config)
        securities       = load_securities (app_config, syncdb) # hyp.: syncdb.load_securities(app_config) ? (no) TODO 
        found            = False
        default_fromdate = datetime.datetime.strptime(default_fromdate, '%Y-%m-%d').date() ##

        #print(str(type(asked_todate)) + ' - ' + str(type(default_fromdate)))

        # load_datafeeds(securities)
        #
        for security_id, _struct in securities.items():
            # attenzione :
            # l'update del record si può fare sempre dal momento che in generale ci si aspetta che ad ogni invocazione
            # per lo meno _struct.todate cambi rispetto al valore presente sul record
            #
            # TODO : il par. path è un attr. di syncdb .. ?
            # TODO hyp: datafile = syncdb.select_security_datafeed(_struct, security_id, default_fromdate, asked_todate, path)
            #file_found, _fromdate, _todate = syncdb.select_security_datafeed(_struct, security_id, default_fromdate, asked_todate, path) 
            datafile = syncdb.select_security_datafeed(_struct, security_id, default_fromdate, asked_todate, path) 
            #if file_found:    
            if datafile:    
                # TODO hyp. nn serve più
                #datafile = path + security_id + '.' + str(_fromdate) + '.' + str(_todate) + '.csv' 
                data = btfeeds.YahooFinanceCSVData (dataname=datafile,    #+#
                                                    #fromdate=datetime.datetime.strptime(_fromdate, '%Y-%m-%d'),
                                                    fromdate=default_fromdate,
                                                    todate=asked_todate + datetime.timedelta(days=1), 
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
                                    fromdate=default_fromdate, 
                                    todate=asked_todate)

            cerebro.run()
            #cerebro.plot(style='candlestick', barup='green', bardown='black')
            syncdb.close()
        else:
            log.info('~ No security found ~')

        log.info('*** END SESSION ***')

    except Exception as e: #BacktraderError as e ?
        #log.exception(e)
        log.error('Abnormal END : {}'.format(e))
        try:
            syncdb.close()
        except Exception:
            pass
        sys.exit(1)


if __name__ == '__main__':
    main()
