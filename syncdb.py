#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, re
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
            self.log.error('invalid syncdb file name : <{}>'.format(self.db_dir + self.db_file))
            raise e
            #sys.exit(1) # TODO gestire questa eccezione in uscita


    def create_default_schema(self):

        schema_file = self.db_dir + 'default_syncdb_schema.sql'
        self.log.info('syncdb NOT FOUND : create default schema <{}>'.format(schema_file))
        try:
            with open(schema_file, 'rt') as f:
                schema = f.read()
                self.conn.executescript(schema)
        
        except FileNotFoundError as e: # [Errno 2] No such schema 
            self.log.error('schema sql file NOT found : {}'.format(e))
            #raise NoSqlFileFound   (custom exception TBD)
            raise e

        except Exception as e: # TBD errori sql nello script
            self.log.error('sql error on schema sql file : {}'.format(e))
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
        # string (filename) or None

        def check_datapoints(f, fromdate, todate):
            '''
            verifica la presenza di records in f compresi tra fromdate/asked_todate

            TEST : 
            non è presente il file default_fromdate..asked_todate ma è presente un file
            che include il suddetto intervallo; 
            se questo file è parziale (non contiene tutti i records relativi al suo periodo dichiarato nel nome
            tipicamente per assenza degli stessi dovuta a inizio/fine quotazione)
            è necessario verificare la presenza di record compresi nel periodo richiesto poichè
            è possibile il caso in cui non ce ne siano e questo produce l'IndexError su cerebro.run()
            '''
            data_expr = r'^([^,]+).+$' # regex per estrarre il campo data
            with open(f, "rb") as f:

                header = f.readline()
                first = f.readline()
                if first:
                    f.seek(-2, 2)
                    try:
                        while f.read(1) != b"\n":
                            f.seek(-2, 1)
                    except OSError as e:
                        print('OsError: {}'.format(e))

                    last = f.readline()
                    #self.log.debug('FIRST : {}'.format(first.decode('utf-8')))
                    #self.log.debug('LAST  : {}'.format(last.decode('utf-8')))
                    #self.log.debug('from date : {}'.format(re.findall(data_expr, first.decode('utf-8'))))
                    #self.log.debug('to date   : {}'.format(re.findall(data_expr, last.decode('utf-8'))))
                    _fromdate           = re.findall(data_expr, first.decode('utf-8'))[0]
                    _todate             = re.findall(data_expr, last.decode('utf-8'))[0]
                    self.log.debug('first datapoint : <{}>'.format(_fromdate))
                    self.log.debug('last  datapoint : <{}>'.format(_todate))

                    # TODO
                    # controllare se la regex va in errore (datapoint/record non valido)
                    #
                    dt_file_fromdate    = datetime.datetime.strptime(_fromdate, FORMAT).date()
                    dt_file_todate      = datetime.datetime.strptime(_todate, FORMAT).date()

                    if dt_file_fromdate <= fromdate and dt_file_todate >= todate:
                        # ci sono datapoints validi
                        return True
                    else:
                        # no valid datapoints found
                        return False

                else:
                    # No records found
                    return False



        self.log.info('select_file(<{}>'.format(security_id))

        FORMAT = '%Y-%m-%d'
        f = path.strip() + security_id + '.' + str(fromdate) + '.' + str(todate) + '.csv'
        self.log.debug('il file cercato è {}'.format(f))

        if os.path.isfile(f):
            if os.path.getsize(f):
                # TODO DEVE controllare se ci sono almeno 2 righe... (1 record)
                # potrei chiamare if check_datapoints(...) come sotto
                #
                self.log.debug('file NON vuoto')
                #self.log.debug('Perfect file MATCH')
                return f
            else:
                self.log.warning('<{}> è vuoto : controllare security_id <{}>'.format(f, security_id))
                os.remove(f)

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
                self.log.info('<{}> cover the asked analisys period'.format(fname))

                if check_datapoints (path.strip()+fname, fromdate, todate):
                    return path.strip() + fname
                else:
                    self.log.warning('NO datapoints found on file {}'.format(fname))
                
                #return path.strip() + fname # TODO sostituire con la if sopra
            else:
                self.log.warning('<{}> do NOT cover the asked period'.format(fname))

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
        # string (datafile) or None

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

        def yahoo_csv_download(security_id, fromdate, todate, datafile):
           return subprocess.call(['../yahoodownload.py',
                            '--ticker', security_id, \
                            '--fromdate', str(fromdate), \
                            '--todate', str(todate + datetime.timedelta(days=1)), \
                            '--outfile', str(datafile)])


        FORMAT = '%Y-%m-%d' # TODO

        self.log.info('select_security_datafeed(<{}>)'.format(security_id))

        # in modalità 'strict' security_id deve esistere su syncdb.securities 
        # 
        if self.strict :
           if _struct is not None:
               # (update)
               file_cached = self.select_file(security_id, fromdate, todate, path)
               if file_cached:
                   _update(security_id, fromdate, todate) # ..... TODO TEST
                   return file_cached
               else:
                   datafile = path + security_id + '.' + str(fromdate) + '.' + str(todate) + '.csv'
                   self.log.debug('SEGUE download sul file <{}>'.format(datafile))
                   if yahoo_csv_download(security_id, fromdate, todate, datafile) != 0:
                       self.log.warning('FAIL to download data for security {}'.format(security_id))
                       try:
                           os.remove(datafile)
                       except Exception as e:
                           pass
                       return None
                   else:
                       if os.path.getsize(datafile):
                           # dovrebbe controllare se ci sono almeno 2 righe...
                           self.log.debug('<{}> NON vuoto'.format(datafile))
                       else:
                           self.log.warning('<{}> è vuoto : controllare security_id <{}>'.format(datafile, security_id))
                           os.remove(datafile)
                           return None

                       _upsert(security_id, fromdate, todate)
                       return datafile
           else:
               self.log.warning('WARNING : unknow security <{}>'.format(security_id))
               return None
        else:
            # (upsert)
            file_cached = self.select_file(security_id, fromdate, todate, path)
            if file_cached:
                _upsert(security_id, fromdate, todate)
                return file_cached
            else:
                datafile = path + security_id + '.' + str(fromdate) + '.' + str(todate) + '.csv'
                self.log.debug('SEGUE download sul file <{}>'.format(datafile))
                if yahoo_csv_download(security_id, fromdate, todate, datafile) != 0:
                    self.log.warning('FAIL to download data for security {}'.format(security_id)) # ex DownloadFailException
                    try:
                        os.remove(datafile)
                    except Exception as e:
                        pass
                    return None
                else:
                    if os.path.getsize(datafile):
                        # dovrebbe controllare se ci sono almeno 2 righe...
                        self.log.debug('<{}> NON vuoto'.format(datafile))
                    else:
                        self.log.warning('<{}> è vuoto : controllare security_id <{}>'.format(datafile, security_id))
                        os.remove(datafile)
                        return None

                    _upsert(security_id, fromdate, todate)
                    return datafile


    def close(self):
        try:
            self.conn.close()
            self.log.info('syncdb <' + self.db_file + '> CLOSED')
        except Exception as e:
            raise e

