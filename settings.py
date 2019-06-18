#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import sys, logging
from configparser import ConfigParser as ConfigParser

__all__ = ['app', 'override_defaults', 'defaults']

''' sections (uppercase)
'''
_OPTIONS_           = 'OPTIONS'
_STORAGE_           = 'STORAGE'
_STRATEGIES_        = 'STRATEGIES'
_DATAFEEDS_         = 'DATAFEEDS'
_SECURITIES_        = 'SECURITIES'

'''options (lowercase)
'''
_strict_            = 'strict'
_syncdb_            = 'syncdb'
_parquet_           = 'parquet'
_yahoo_csv_data_    = 'yahoo_csv_data'
_securities_        = 'securities'
_fromdate_          = 'fromdate'

''' sections/options composition
'''
_KV_OPTIONS_        = {_strict_ : 'yes'}
_KV_STORAGE_        = {_syncdb_ : None, _parquet_ : None, _yahoo_csv_data_ : None}
_KV_STRATEGIES_     = dict()
_KV_DATAFEEDS_      = {_securities_  : list()}
_KV_SECURITIES_     = {_fromdate_ : '2010-12-31'}

'''sections che possono definire opzioni arbitrarie
'''
no_strict_section = (
            _STRATEGIES_,
            )

'''DEFAULT SETTINGS
'''
defaults = {
        _OPTIONS_        : _KV_OPTIONS_,
        _STORAGE_        : _KV_STORAGE_,
        _STRATEGIES_     : _KV_STRATEGIES_,
        _DATAFEEDS_      : _KV_DATAFEEDS_,
        _SECURITIES_     : _KV_SECURITIES_
           }

''' app config sections
'''
app = frozenset(defaults.keys())    #TODO

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

def merge_settings (defaults, configured, debug=False): 
    '''
    defaults            :   defaults configuration dict
    configured          :   app configuration dict (based on ConfigParser object)
    debug               :   True => some additional debug log info 

    return a dict with all defaults eventually updated with configured items
    '''

    func_name = sys._getframe().f_code.co_name
    log = logging.getLogger(__name__)
    log.info('==> Running {}()'.format(func_name))

    run_settings    = {}

    try:
        for section, options in defaults.items():

            _msg = 'default section : [{}]'.format(section)

            if section in configured:
                log.debug(_msg + ' + found in config')
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
                #raise

    if debug:
        log.debug('merged configuration :')
        for section, options in run_settings.items(): log.debug('[{}] = {}'.format(section, options)) 

    log.info('<== leave {}()'.format(func_name))

    return run_settings


def getlist(option, sep=',', chars=None):
    '''
    Return a list from a ConfigParser option. By default,
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


#def overriden_by(modifiers):
def override_defaults(modifiers):

    run_settings = dict()
    passed       = False

    if type(modifiers) is list:
        for modifier in modifiers:
            if type(modifier) is ConfigParser:
                # https://stackoverflow.com/questions/1773793/convert-configparser-items-to-dictionary
                # config_parser_dict = { s : dict(modifier.items(s)) for s in modifier.sections() }
                # modifier.items(s) ==> lista di tuple : contiene una tupla per ogni option : ('option', 'value')
                # es.: [('syncdb', '/path/to/local_storage/'), ('parquet', '/path/to/parquet/'), ('yahoo_csv_data', '/path/to/yahoo_csv_cache/')]

                configured = { s : parse_items(modifier.items(s)) for s in modifier.sections() }
                print('configparser dictionary :')
                print(configured)
                print('...segue merge configurazione')
                run_settings = merge_settings(defaults, configured, debug=True)
                if run_settings:
                    passed = True
            else:
                print(str(type(modifier)) + ' is not a configparser')
    else:
        log.warning('overriden_by() params is NOT valid : use defaults')
        run_settings = defaults

    if not passed:
        run_settings = defaults

    return run_settings
