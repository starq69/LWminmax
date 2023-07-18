#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, re
import fnmatch
import sqlite3
import datetime
import logging, logging.config, configparser
import subprocess
from datasource import datasource

'''
class DownloadFailException(Exception):
    pass
'''

def get_file_items (path, pattern=None, sort=True, fullnames=True):

    if not pattern: pattern = '*'
    _items = []

    if type (pattern) is list:

        for p in pattern:
            if fullnames:
                _items += [f.path for f in os.scandir(path) if f.is_file() and fnmatch.fnmatch(f.name, p)]
            else:
                _items += [f.name for f in os.scandir(path) if f.is_file() and fnmatch.fnmatch(f.name, p)]
    else:
        if fullnames:
            _items = [f.path for f in os.scandir(path) if f.is_file() and fnmatch.fnmatch(f.name, pattern)]
        else:
            _items = [f.name for f in os.scandir(path) if f.is_file() and fnmatch.fnmatch(f.name, pattern)]

    if sort:
        _items=sorted(_items)

    return _items


class duckdb_data(datasource):

    def __init__(self, db_dir=None, db_file=None, path=None, strict=False):

        self.log = logging.getLogger (__name__)
        super().__init__(db_dir, db_file, path, strict) # validation


    def load_securities(self, securities):

        _securities = {code: None for code in securities}
        #not_found = securities[:]   # copy
        self.log.debug('securities : <{}>'.format(_securities))

        return _securities 


    def select_file(self, security_id, fromdate=None, todate=None, path=None): # add FORMAT param TODO
        #
        # controllare path : TODO
        # in:
        # security_id       : string
        # fromdate/todate   : datetime.date 
        #
        # out:
        # string (filename) or None

        self.log.info('select_file(<{}>'.format(security_id))

        return None


    '''
    TODO
    cambiare il nome
    '''
    def select_security_datafeed(self, _struct, security_id):
        #
        # in:
        # _struct           : dict or None
        # security_id       : string
        # fromdate/todate   : datetime.date
        # path              : string
        #
        # out:
        # string (datafile) or None
        func_name = sys._getframe().f_code.co_name
        self.log.debug(func_name)


    def close(self):
        pass