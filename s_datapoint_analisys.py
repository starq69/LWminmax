#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
backtrader strategy test main module (run.py)
'''
import os, sys
import datetime
from loader import load_module
import logging, logging.config, configparser
import backtrader as bt
import backtrader.feeds as btfeeds
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# tnks to Wes McKinney (https://wesmckinney.com/blog/python-parquet-update/)
#
def write_to_parquet(df, out_path, compression='SNAPPY'):
    arrow_table = pa.Table.from_pandas(df)
    if compression == 'UNCOMPRESSED':
        compression = None
    pq.write_table(arrow_table, out_path, use_dictionary=False,
                   compression=compression)


def isNaN(num):
    return num != num


def get_strategy_class():
    return S_Datapoint_Analisys


class S_Datapoint_Analisys(bt.Strategy):

    def __init__(self, config=None, name=None, fromdate=None, todate=None):

        self.name = name    # TODO controllare se None...

        self.log = logging.getLogger (__name__)
        self.log.info('__init__ Strategy <{}> <{}>'.format(repr(self.__class__), self.name))

        if fromdate is not None and isinstance(fromdate, datetime.date):
            self.fromdate = fromdate.strftime('%Y-%m-%d')
        else:
            self.log.error('Invalid fromdate par. to strategy {}'.format(self.name))
        if todate is not None and isinstance(todate, datetime.date):
            self.todate = todate.strftime('%Y-%m-%d')
        else:
            self.log.error('Invalid todate par. to strategy {}'.format(self.name))

        # TODO
        # dopo l'introduzione dei settings/conventions config qui ora è un dict
        if config is not None and isinstance(config, configparser.ConfigParser):
            try:
                configured_indicators = [_ind.strip() for _ind in config.get('STRATEGIES', name).split(',') if len(_ind)]
            except configparser.NoOptionError as e:
                # TODO possibile skip della stategia ?
                self.log.error('No indicators found for strategy <{}> : {}'.format(name, e))
                sys.exit(1)
            try:
                self.parquet_storage       = config.get('STORAGE', 'parquet')
            except configparser.NoOptionError as e:
                self.log.error('Missing option "parquet" in section "STORAGE" : fix it in order to save indicators result') 
                self.parquet_storage = None
                # raise ?

        else:
            print('invalid **kwarg params passed to <' + repr(self.__class__) + '> instance')
            sys.exit(1)

        self.loop_count = 0
        
        self.indicators     = dict() ##

        for i_name in configured_indicators:
            try:
                self.indicators[i_name] = dict()
                _mod        = self.indicators[i_name]['__module__'] = load_module(i_name) # .. può restituire direttamente l'istanza dell'indicatore?
                ind_class   = self.indicators[i_name]['__class__']  = _mod.get_indicator_class()

                for _, datafeed in enumerate(self.datas):
                    self.indicators[i_name][datafeed._name] = dict()
                    self.indicators[i_name][datafeed._name]['indicator_instance'] = ind_class(datafeed, strategy=self)
                    self.indicators[i_name][datafeed._name]['output_dataframe']   = pd.DataFrame()

            except Exception as e:  # ModuleNotFoundError ...
                # gli indicatori non validi sono scartati e non pregiudicano l'esecuzione
                self.log.error(e)
            

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

        self.loop_count += 1
        ### print max/min for log analisys purpose
        #
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
        for indicator, _dict in self.indicators.items():
            for item, detail in _dict.items():
                try:
                    # salva il dataframe in formato parquet
                    #
                    if self.parquet_storage is not None: # TODO meglio prevedere un default...
                        # TODO in attesa di meglio :
                        # ricava uno short name dell'indicatore dal par. indicator (vedi _name sull'impl. della classi indicatore)
                        #
                        ind_shortname = indicator[:-9]          # rimuove 'Indicator' alla fine
                        ind_shortname = ind_shortname[2:]       # rimuove 'I_' all'inizio
                        ind_shortname = ind_shortname.upper()   
                        pq_fname = self.parquet_storage + str(item) + '.' + self.fromdate + '.' + self.todate + '.' + ind_shortname + '.parquet'
                        write_to_parquet (detail['output_dataframe'], pq_fname)
                        self.log.info('pyarrow.parquet.write_table <{}> DONE'.format(pq_fname))
                except FileNotFoundError as e:
                    self.log.error(e) 
                except Exception:
                    pass

        self.log.info('Exit Strategy <{}> {}, strategy.next loop_count = {}'.format(self.name, repr(self.__class__), str(self.loop_count)))
