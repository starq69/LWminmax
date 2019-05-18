#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys 
import fnmatch
import sqlite3
import datetime
import logging, logging.config, configparser
import subprocess

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


class syncdb():

    def __init__(self, db_dir=None, db_file=None, strict=False):

        self.log = logging.getLogger (__name__)

        self.strict = strict
        self.db_dir = db_dir

        if db_dir and db_file: 
            self.db_dir  = db_dir.strip()
            self.db_file = db_file.strip()
            db_filename  = self.db_dir + self.db_file 
            # http://robyp.x10host.com/sqlite3.html#loaded
            db_is_new    = not os.path.exists(db_filename)
            try:
                self.conn = sqlite3.connect(db_filename)
                if db_is_new:
                    self.create_default_schema()
                else:
                    self.log.info('syncdb <' + db_filename + '> CONNECTED')

            except sqlite3.OperationalError as e:
                raise e
        else:
            self.log.exception('invalid syncdb file name : <{}>'.format(self.db_dir + self.db_file))
            sys.exit(1) # TODO gestire questa eccezione in uscita


    def create_default_schema(self):

        schema_file = self.db_dir + 'default_syncdb_schema.sql'
        self.log.info('syncdb NOT FOUND : create default schema <{}>'.format(schema_file))
        try:
            with open(schema_file, 'rt') as f:
                schema = f.read()
                self.conn.executescript(schema)
        
        except FileNotFoundError as e: # [Errno 2] No such schema 
            self.conn.close()   # TODO TEST
            #raise NoSqlFileFound   (custom exception TBD)
            raise e

        except Exception as e: # TBD errori sql nello script
            self.conn.close()   # TODO TEST
            raise e


    def load_securities(self, securities):

        _securities = {code: None for code in securities}
        #not_found = securities[:]   # copy

        try:
            cursor = self.conn.cursor()
            query = f"SELECT code, start_date, end_date FROM securities WHERE code IN ({','.join(['?']*len(securities))})"
            cursor.execute(query, securities)

            for row in cursor.fetchall():
                code, start_date, end_date      = row
                _securities[code]               = dict()
                _securities[code]['start_date'] = start_date
                _securities[code]['end_date']   = end_date
                #not_found.remove(code)

        except Exception as e:
            raise e

        return _securities 


    def select_file(self, security_id, fromdate, todate, path=None): # add FORMAT param TODO
        #
        # controllare path : TODO
        # in:
        # security_id       : string
        # fromdate/todate   : datetime.date 
        #
        # out:
        # bool, string or None, string or None
        # hyp: TODO
        # filename or None
        #
        # NB. TODO
        # il par. todate non verrà più incrementato di 1 gg e NON ci sono impatti qui

        FORMAT = '%Y-%m-%d'
        f = path.strip() + security_id + '.' + str(fromdate) + '.' + str(todate) + '.csv'
        self.log.debug('il file cercato è {}'.format(f))

        try:
            _, fname    = os.path.split(f)
        except Exception as e:
            self.log.error('exception in select_file() : ' + str(e))
            #return # TODO hyp: return None
            return None
            
        if os.path.isfile(f):   # perfect match (from/to date)
            self.log.debug('Perfect file MATCH')
            #return True, str(fromdate), str(todate) # TODO Hyp: return f
            return f
        else:
            # cerca il/i file del tipo <security_id>.<from>.<to>.csv e verifica la copertura del periodo richiesto
            #
            self.log.debug('segue get_file_items()')
            flist = get_file_items(path, security_id+'.'+'*.csv', fullnames=False)
            self.log.debug(flist)
            for fname in flist:
                _parts = fname.split('.')
                file_fromdate = _parts[1]
                file_todate   = _parts[2]
                dt_file_fromdate = datetime.datetime.strptime(file_fromdate, FORMAT).date()
                dt_file_todate   = datetime.datetime.strptime(file_todate, FORMAT).date()
                if dt_file_fromdate <= fromdate and dt_file_todate >= todate:
                    self.log.debug('<{}> cover the asked analisys period'.format(fname))
                    #return True, file_fromdate, file_todate # TODO hyp: return path + fname
                    return path.strip() + fname
                else:
                    self.log.debug('<{}> do NOT cover the asked period'.format(fname))

            #return False, None, None # TODO hyp: return None
            return None


    def select_security_datafeed(self, _struct, security_id, fromdate, todate, path):
        #
        # in:
        # _struct           : dict or None
        # security_id       : string
        # fromdate/todate   : datetime.date
        # path              : string
        #
        # out:
        # bool, datetime.date or None, datetime.date or None 

        # TODO
        # ATTENZIONE :
        # todate è incrementato di un gg. e non va bene nell'update/upsert (risulta avanti di 1 gg risp. al file)
        # todate (effettivo) deve essere incrementato di un gg. soltato quando è utilizzato come par. --todate nella subprocess.call
        # CORREGGERE di conseguenza (vedi anche main())

        def _upsert(security_id, fromdate, todate):
            sql=f"update securities set start_date=:fromdate, end_date=:todate where code=:security_id"
            self.conn.execute(sql, {'security_id':security_id, 'fromdate':fromdate, 'todate':todate})
            sql=f"insert into securities (code, start_date, end_date) select :security_id, :fromdate, :todate where (select Changes() = 0)"
            self.conn.execute(sql, {'security_id':security_id, 'fromdate':fromdate, 'todate':todate})
            self.conn.commit()

        def _update(security_id, fromdate, todate):
            sql=f"update securities set start_date=:fromdate, end_date=:todate where code=:security_id"
            self.conn.execute(sql, {'security_id':security_id, 'fromdate':fromdate, 'todate':todate})
            self.conn.commit()

        #print('syncdb.select_security_datafeed() type of fromdate/todate are : {} {}'.format(type(fromdate), type(todate)))
        #print('syncd.select_security_datafeed() _struct/security_id/path : {} {} {}'.format(type(_struct), type(security_id), type(path)))

        FORMAT = '%Y-%m-%d' # TODO

        # in modalità 'strict' security_id deve esistere su syncdb.securities 
        # 
        if self.strict :
           if _struct is not None:
               # (update)
               # TODO hyp: file_cached = self.select_file(security_id, fromdate, todate, path)
               #file_cached, file_fromdate, file_todate = self.select_file(security_id, fromdate, todate, path)  
               file_cached = self.select_file(security_id, fromdate, todate, path)
               if file_cached:
                   _update(security_id, fromdate, todate) # ..... TODO TEST
                   # TODO hyp: return file_cached
                   #return True, datetime.datetime.strptime(file_fromdate, FORMAT).date(), datetime.datetime.strptime(file_todate, FORMAT).date()
                   return file_cached
               else:
                   datafile = path + security_id + '.' + str(fromdate) + '.' + str(todate) + '.csv'
                   self.log.debug('SEGUE download sul file <{}>'.format(datafile))
                   c=subprocess.call(['../yahoodownload.py',
                                    '--ticker', security_id, \
                                    '--fromdate', str(fromdate), \
                                    #'--todate', str(todate), \
                                    '--todate', str(todate + datetime.timedelta(days=1)), \
                                    '--outfile', datafile])            #+#
                   if c != 0:
                       self.log.warning('FAIL to download data for security {}'.format(security_id))
                       #return False, None, None # TODO hyp: return None/False
                       return None
                   else:
                       _upsert(security_id, fromdate, todate)
                       #return True, fromdate, todate    # TODO hyp : return datafile
                       return datafile
           else:
               self.log.warning('WARNING : unknow security <{}>'.format(security_id))
               #return False, None, None # TODO hyp: return None/false
               return None
        else:
            # (upsert)
            # TODO hyp: file_cached = self.select_file(security_id, fromdate, todate, path)
            #file_cached, file_fromdate, file_todate = self.select_file(security_id, fromdate, todate, path)
            file_cached = self.select_file(security_id, fromdate, todate, path)
            if file_cached:
                _upsert(security_id, fromdate, todate)
                # TODO hyp: return file_cached
                #return True, datetime.datetime.strptime(file_fromdate, FORMAT).date(), datetime.datetime.strptime(file_todate, FORMAT).date()
                return file_cached
            else:
                #datafile = path + security_id + '.' + str(fromdate) + '.' + str(todate - datetime.timedelta(days=1)) + '.csv'
                datafile = path + security_id + '.' + str(fromdate) + '.' + str(todate) + '.csv'
                self.log.debug('SEGUE download sul file <{}>'.format(datafile))
                c=subprocess.call(['../yahoodownload.py',
                                 '--ticker', security_id, \
                                 '--fromdate', str(fromdate), \
                                 #'--todate', str(todate), \
                                 '--todate', str(todate + datetime.timedelta(days=1)), \
                                 '--outfile', datafile])            #+#
                if c != 0:
                    # TODO qui il file viene cmq creato vuoto e fa fallire il test al successivo run :
                    # TODO nella select_file() restituire None se il file esiste ed è vuoto
                    # testato in strict=no con security inesistente (codice errato)
                    self.log.warning('FAIL to download data for security {}'.format(security_id)) # ex DownloadFailException
                    #return False, None, None # TODO hyp: return None/False
                    return None
                else:
                    _upsert(security_id, fromdate, todate)
                    #return True, fromdate, todate # TODO hyp : return datafile
                    return datafile


    def close(self):
        try:
            self.conn.close()
            self.log.info('syncdb <' + self.db_file + '> CLOSED')
        except Exception as e:
            raise e

