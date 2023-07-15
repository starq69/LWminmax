#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import os, sys, logging, argparse, datetime as dt
from configparser import ConfigParser as ConfigParser
from pathlib import Path


__all__ = ['app', 
           'override_defaults', 
           'defaults', 
           'args_parser', 
           '_ini_settings_file_', 
           '_log_settings_file_', 
           '_default_ini_settings_file_', 
           '_default_log_settings_file_',
           'parent_dir',
           '_INTERNALS_',
           '_OPTIONS_',
           '_STORAGE_',
           '_STRATEGIES_',
           '_DATAFEEDS_',
           '_SECURITIES_']

yesterday = dt.datetime.strftime(dt.date.today() - dt.timedelta(days=1),'%Y-%m-%d')

''' sections (uppercase)
'''
_INTERNALS_         = 'INTERNALS'
_OPTIONS_           = 'OPTIONS'
_STORAGE_           = 'STORAGE'
_STRATEGIES_        = 'STRATEGIES'
_DATAFEEDS_         = 'DATAFEEDS'
_SECURITIES_        = 'SECURITIES'

'''options (lowercase)
'''
_configparser_as_dict_      = 'configparser_as_dict'
_ini_settings_file_         = 'globalconfig'
_log_settings_file_         = 'logconfig' 
_strict_                    = 'strict'
_syncdb_                    = 'syncdb'
_parquet_                   = 'parquet'
_yahoo_csv_data_            = 'yahoo_csv_data'
_duckdb_data_               = 'duckdb_data'
_securities_                = 'securities'
_source_                    = 'source'
_timeframe_                 = 'timeframe'
_fromdate_                  = 'fromdate'
_todate_                    = 'todate'

''' sections/options composition
'''
#base_dir        = os.path.dirname (os.path.realpath(__file__))
base_dir        = Path.cwd()    #starq@new --> can also be Path(__file__) ?
#parent_dir      = os.path.split (base_dir)[0]
parent_dir      = base_dir.parent

#local_storage   = parent_dir + '/local_storage/'
local_storage   = parent_dir / 'local_storage'

#default_source  = parent_dir + '/local_storage/yahoo_csv_cache/'
default_source  = local_storage / 'yahoo_csv_cache'

#_KV_INTERNALS_      = {_configparser_as_dict_ : 'yes', _ini_settings_file_ : parent_dir + '/app.ini', _log_settings_file_ : parent_dir + '/log.ini'}
_KV_INTERNALS_      = {_configparser_as_dict_ : 'yes', _ini_settings_file_ : parent_dir / 'app.ini', _log_settings_file_ : parent_dir / 'log.ini'}

_KV_OPTIONS_        = {_strict_ : 'yes'}

_KV_STORAGE_        = {
                        _syncdb_            : local_storage, 
                        #_parquet_           : local_storage + '/parquet/', 
                        _parquet_           : local_storage / 'parquet',
                        #_yahoo_csv_data_    : local_storage + '/yahoo_csv_cache/',
                        _yahoo_csv_data_    : local_storage / 'yahoo_csv_cache',
                        #_duckdb_data_       : local_storage + '/duckdb_data/'
                        _duckdb_data_       : local_storage / 'duckdb_data'
                      }

_KV_STRATEGIES_     = dict()
_KV_DATAFEEDS_      = {_securities_  : list(), _source_ : default_source}
#_KV_SECURITIES_     = {_fromdate_ : '2017-12-31', _todate_ : yesterday} 
_KV_SECURITIES_     = {_fromdate_ : None, _todate_ : None, _timeframe_ : 'M1'}

'''sections che possono definire opzioni arbitrarie
'''
no_strict_section = (
            _STRATEGIES_,
            )

'''DEFAULT SETTINGS
'''
defaults = {
        _INTERNALS_      : _KV_INTERNALS_,
        _OPTIONS_        : _KV_OPTIONS_,
        _STORAGE_        : _KV_STORAGE_,
        _STRATEGIES_     : _KV_STRATEGIES_,
        _DATAFEEDS_      : _KV_DATAFEEDS_,
        _SECURITIES_     : _KV_SECURITIES_
           }

''' app config sections
'''
app = frozenset(defaults.keys())

_default_ini_settings_file_   = defaults[_INTERNALS_][_ini_settings_file_]
_default_log_settings_file_   = defaults[_INTERNALS_][_log_settings_file_]
''' 
https://stackoverflow.com/questions/39440190/a-workaround-for-pythons-missing-frozen-dict-type

https://stackoverflow.com/questions/2703599/what-would-a-frozen-dict-be
VERIFICARE QUESTA SOLUZIONE:

from types import MappingProxyType

default_config = {'a': 1}
DEFAULTS = MappingProxyType(default_config)

def foo(config=DEFAULTS):
    ...
'''

def args_parser(pargs=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=(
            'Multiple Values and Brackets'
        )
    )
    '''https://stackoverflow.com/questions/30487767/check-if-argparse-optional-argument-is-set-or-not'''
    parser.add_argument('--fromdate',       default=argparse.SUPPRESS, help='Date in YYYY-MM-DD format')
    parser.add_argument('--todate',         default=argparse.SUPPRESS, help='Date in YYYY-MM-DD format')
    parser.add_argument('--timeframe',      default=argparse.SUPPRESS, choices=['M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'D1'])
    parser.add_argument('--strict',         default=argparse.SUPPRESS, choices=['yes', 'no', '1', '0', 'true', 'false', 'on', 'off'], help='strict can be yes/1/true/on or no/0/false/off')
    parser.add_argument('--globalconfig',   default=argparse.SUPPRESS)
    parser.add_argument('--logconfig',      default=argparse.SUPPRESS)

    return parser.parse_args()


def merge_settings (defaults, configured, debug=False): 
    '''
    defaults            :   defaults configuration dict
    configured          :   app configuration dict (based on ConfigParser object)
    debug               :   True => some additional debug log info 

    return a dict with all defaults eventually updated with configured items
    '''
    #func_name = sys._getframe().f_code.co_name
    log = logging.getLogger(__name__)
    #log.info('==> Running {}()'.format(func_name))

    run_settings    = {}

    try:
        for section, options in defaults.items():

            _msg = 'default section : [{}]'.format(section)

            if section in configured:
                #log.debug(_msg + ' + found in config')
                run_settings[section] = defaults[section]
                for option, value in configured[section].items():
                    if option in run_settings[section] or section in no_strict_section:    ##
                        run_settings[section][option] = value
                    else:
                        log.warning('UNKNOW option <{}> in section [{}]'.format(option, section))
            else:
                log.debug(_msg + ' - NOT found in config')
                run_settings[section] = defaults[section]

    except Exception as e:
                log.error('merge_policy exception : {}'.format(e))
                run_settings = None

    if debug:
        log.debug('merged configuration :')
        for section, options in run_settings.items(): log.debug('[{}] = {}'.format(section, options)) 

    #log.info('<== leave {}()'.format(func_name))

    return run_settings


def merge_arguments(defaults, args):

    log = logging.getLogger(__name__)
    #print('ARGS : <{}>'.format(args))
    if type(defaults) is dict:
        keys        = []
        sections    = {}
        for section, options in defaults.items():
            keys += [*options]
            for option in options:
                sections[option] = section

        for karg, arg in args.items():
            if karg in keys:
                #print('KEY ARGUMENT : <{}> dovrà essere sovrascritto'.format(karg))
                defaults[sections[karg]][karg] = arg
            else:
                log.warning('UNKNOW Argument <{}>'.format(karg))


def getlist(option, sep=',', chars=None):
    '''Return a list from a ConfigParser option. By default,
    split on a comma and strip whitespaces.
    Return a stripped string if sep is not found in option.
    '''
    if option.find(sep) >= 0:
        return [ chunk.strip(chars) for chunk in option.split(sep) ]
    else:
        return option.strip(chars)


def parse_items(options):
    '''
    option      : ConfigParser.options(<section>) ==> lista di tuple
    '''
    _dict = dict()
    for item in options:
        _dict[item[0]] = getlist(item[1])

    return _dict


def override_defaults(modifiers):

    log = logging.getLogger(__name__)

    run_settings = None
    passed       = False

    if type(modifiers) is list:
        for modifier in modifiers:
            _type = type(modifier)
            if _type is ConfigParser:
                ''' https://stackoverflow.com/questions/1773793/convert-configparser-items-to-dictionary
                config_parser_dict = { s : dict(modifier.items(s)) for s in modifier.sections() }
                modifier.items(s) ==> lista di tuple : contiene una tupla per ogni option : ('option', 'value')
                es.: [('syncdb', '/path/to/local_storage/'), ('parquet', '/path/to/parquet/'), ('yahoo_csv_data', '/path/to/yahoo_csv_cache/')]
                '''
                configured      = { s : parse_items(modifier.items(s)) for s in modifier.sections() }
                run_settings    = merge_settings (defaults, configured) #, debug=True)
                if run_settings:
                    passed = True
                    '''
                    log.info('validazione intervallo [fromdate..todate] ...')
                    if (dt.datetime.strptime(run_settings[_SECURITIES_][_fromdate_], '%Y-%m-%d').date() < \
                        dt.datetime.strptime(run_settings[_SECURITIES_][_todate_], '%Y-%m-%d').date()):
                        passed = True
                    else:    
                        log.error('INVALID PERIOD : {} - {}'.format(run_settings[_SECURITIES_][_fromdate_], \
                                                                    run_settings[_SECURITIES_][_todate_]))

                        passed = False
                    '''
            elif _type is argparse.Namespace:
                if run_settings:
                    configured = merge_arguments(run_settings, vars(modifier))
                else:
                    configured = merge_arguments(defaults, vars(modifier))
                passed = True

            else:
                log.error('invalid param passed to override_defaults()')
    else:
        log.warning('overriden_by() params is NOT valid : use defaults')
        run_settings = defaults

    if not passed:
        run_settings = defaults

    debug=True #
    if debug:
        log.debug('RUN SESSION SETTINGS ({}):'.format(str(passed)))
        for section, options in run_settings.items(): log.debug('[{}] = {}'.format(section, options))     

    return run_settings
