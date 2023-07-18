#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, re
from pathlib import Path
import fnmatch
import datetime as dt
import logging, logging.config
from datasource import datasource
import pandas as pd 
import backtrader as bt
import backtrader.feeds as btfeeds


class MissingBatch(Exception):
    pass


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


class csv_cache(datasource):

    def __init__(self, db_dir=None, db_file=None, path=None, strict=False):

        '''
        hyp.:
        db_dir = default | /daily_csv/ (dal job 002 - csv_split_daily)
        db_file= batch.csv --> prodotto dal job che precede questo (da definire) con l'elenco dei files da elaborare
        path = path del db_file

        '''
        self.log = logging.getLogger (__name__)
        super().__init__(db_dir, db_file, path, strict) # validation
        
        self.db_file = self.db_dir / self.db_file
        if not os.path.exists(self.db_file):
            self.log.warning(f'MISSING batch/db file <{Path(self.db_file)}>')
            raise MissingBatch
        else:
            self.log.warning(f'batch file <{Path(self.db_file)}> FOUND')


    def load_securities(self, securities):

        func_name   = sys._getframe().f_code.co_name
        _securities = dict() # override

        '''
        if os.path.getsize(self.db_file):
            pass
        else:
            pass
        '''

        # load batch into pandas dataframe
        #
        _batch = pd.read_csv(self.db_file, header=None, sep='\t', names=['security', 'day']).to_dict('index')

        for _idx, _values in _batch.items():
            _key = _values['security'] + '.' + str(_idx) 
            _securities[_key] = _values['day'] #+ ' 00:00:00' #starq@new

        self.log.debug(f'<{func_name}> ----> securities : {_securities}')

        # starq@new
        # associo il dizionario all'istanza per impostare fromdate nella parse_data()  
        self.securities = _securities

        return _securities 


    def select_file(self, security_id, fromdate=None, todate=None, path=None): # add FORMAT param TODO

        return None


    '''
    TODO
    cambiare il nome
    '''
    def select_security_datafeed(self, _struct, security_id): #, fromdate=None, todate=None):
        '''
        _struct     : stringa in formato YYYY-MM-DD (è la colonna 'day' in batch.csv)
        security_id : campo 'security' in bacth.csv + '.' + indice del record in bacth.csv : es.: MNQ.0 
        fromdate    : non utilizzata
        todate      : non utilizzata
        '''
        func_name   = sys._getframe().f_code.co_name

        #starq@new
        self.security_id = security_id

        _security = r'^([^.]+).+$'
        security_id = re.findall(_security, security_id)[0]

        _datafile = security_id + '.' + _struct + '.csv'
        _datafile = self.db_dir / _datafile

        self.log.debug(f'<{func_name}> ----> datafile : <{_datafile}>')
        
        return _datafile


    def parse_data(self, datafile): #, run_fromdate=None, run_todate=None):
        '''
        todo:
        ottenere offset dal TZ relativo al giorno : a cura del processo che genera il batch
        '''
        func_name       = sys._getframe().f_code.co_name

        fromdate        =   dt.datetime.strptime(self.securities[self.security_id], '%Y-%m-%d')
        _offset_open    =   dt.timedelta(minutes=870)   # 810min da 00:00 a 14:30
        fromdate        +=  _offset_open
        _session_length =   dt.timedelta(minutes=390)   # 6h 1/2 durata sessione (14:30-21:00)
        todate          =  fromdate + _session_length
        '''
        self.log.debug(f'<{func_name}> ----> fromdate : {fromdate}')        
        self.log.debug(f'<{func_name}> ----> todate   : {todate}')        
        '''
        return btfeeds.GenericCSVData(dataname=datafile,
                                      fromdate=fromdate,
                                      todate=todate,
                                      nullvalue=0.0,
                                      dtformat=('%Y-%m-%d'),
                                      tmformat=('%H:%M:%S'),
                                      timeframe=bt.TimeFrame.Minutes,
                                      separator='\t',
                                      datetime=0,                        
                                      time=1,
                                      open=2,
                                      high=3,
                                      low=4,
                                      close=5,
                                      volume=6,
                                      openinterest=-1)


    def close(self):
        pass