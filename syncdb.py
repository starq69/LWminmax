#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys 
import fnmatch
import sqlite3
from datetime import datetime
import logging, logging.config, configparser
import subprocess

class DownloadFailException(Exception):
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


class syncdb():

    def __init__(self, db_dir=None, db_file=None, strict=False):

        self.log = logging.getLogger (__name__)

        self.strict = strict
        self.db_dir = db_dir

        if db_dir and db_file: 
            self.db_dir  = db_dir.strip()
            self.db_file = db_file.strip()
            db_filename  = self.db_dir + self.db_file 
            db_is_new    = not os.path.exists(db_filename)

            try:
                self.conn = sqlite3.connect(db_filename)

                if db_is_new:
                    self.create_default_schema()
                else:
                    self.log.info('syncdb <' + db_filename + '> CONNECTED')

                #self.conn.close()

            except sqlite3.OperationalError as e:
                raise e
        else:
            self.log.exception('invalid syncdb name! --> ABEND')
            sys.exit(1)


    def create_default_schema(self):

        self.log.debug ('create_default_schema() ....')
        schema_file = self.db_dir + 'default_syncdb_schema.sql'
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
        # rivedere i valori che restituisce #TODO
        #
        FORMAT = '%Y-%m-%d'
        f = path.strip() + security_id + '.' + str(fromdate) + '.' + str(todate) + '.csv'
        self.log.debug('il file cercato è {}'.format(f))

        try:
            _, fname    = os.path.split(f)
        except Exception as e:
            print('exception in select_file() : ' + str(e))
            
        if os.path.isfile(f):   # perfect match (from/to date)
            print('PERFECT FILE MATCH')
            return True, str(fromdate), str(todate) # TODO : ma sono già stringhe ???
        else:
            # cerca il/i file del tipo <security_id>.<from>.<to>.csv e verifica la copertura dei periodi
            #
            print('segue get_file_items()')
            flist = get_file_items(path, security_id+'.'+'*.csv', fullnames=False)
            print(str(flist))
            for fname in flist:
                _parts = fname.split('.')
                file_fromdate = _parts[1]
                file_todate   = _parts[2]
                dt_file_fromdate = datetime.strptime(file_fromdate, FORMAT).date()
                dt_file_todate   = datetime.strptime(file_todate, FORMAT).date()
                if dt_file_fromdate <= fromdate and dt_file_todate >= todate:
                    self.log.debug('{} cover the asked analisys period'.format(fname))
                    return True, file_fromdate, file_todate
                else:
                    print('{} do NOT cover the asked period'.format(fname))

            return False, None, None


    def insert_security(self, _struct, security_id, fromdate, todate, datafile=None):   #OLD
        #
        # rendo transazionale : download + insert/update
        # https://community.backtrader.com/topic/499/saving-datafeeds-to-csv/2
        #
        file_cached, file_fromdate, file_todate = self.select_file(_struct, f=datafile)

        if file_cached:
            self.log.debug('file <{}> is cached!'.format(datafile))
            if _struct is None: # se abbiamo perso il record relativo al file...
                self.log.debug('missing record <{}>'.format(security_id))
                sql=f"insert into securities (code, start_date, end_date) values (:security_id , :fromdate, :todate)"
                self.conn.execute(sql, {'security_id':security_id, 'fromdate':file_fromdate, 'todate':file_todate})
                self.conn.commit()
                self.log.debug('record inserito')
            return
        else:
            # download
            #
            c=subprocess.call(['../yahoodownload.py',
                             '--ticker', security_id, \
                             '--fromdate', fromdate, \
                             '--todate', todate, \
                             '--outfile', datafile])            #+#
            if c != 0:
                raise DownloadFailException
            self.log.info('security <' + security_id + '> download complete('+str(c)+')')

            if _struct is None:
                # insert
                sql=f"insert into securities (code, start_date, end_date) values (:security_id , :fromdate, :todate)"
                self.conn.execute(sql, {'security_id':security_id, 'fromdate':fromdate, 'todate':todate})
                self.conn.commit()
            else:
                #update
                sql=f"update securities set start_date=:fromdate, end_date=:todate where code=:security_id"
                self.conn.execute(sql, {'security_id':security_id, 'fromdate':fromdate, 'todate':todate})
                self.conn.commit()

        return


    def insert_security_ex(self, _struct, security_id, fromdate, todate, path):

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


        # in modalità 'strict' security_id deve esistere su syncdb.securities 
        # 
        if self.strict :
           if _struct is not None:
               # block (update)
               file_cached, file_fromdate, file_todate = self.select_file(security_id, fromdate, todate, path)  ### TODO modificare select_file()
               if file_cached:
                   _update(security_id, fromdate, todate) # ..... TODO TEST
                   return True, file_fromdate, file_todate
               else:
                   datafile = path + security_id + '.' + str(fromdate) + '.' + str(todate) + '.csv'
                   self.log.debug('SEGUE download sul file <{}>'.format(datafile))
                   c=subprocess.call(['../yahoodownload.py',
                                    '--ticker', security_id, \
                                    '--fromdate', str(fromdate), \
                                    '--todate', str(todate), \
                                    '--outfile', datafile])            #+#
                   if c != 0:
                       self.log.warning('FAIL to download data for security {}'.format(security_id))
                       return False
                   else:
                       _upsert(security_id, fromdate, todate)
                       return True, str(fromdate), str(todate)
           else:
               self.log.warning('WARNING : unknow security <{}>'.format(security_id))
               return False
        else:
            # block (upsert)
            file_cached, file_fromdate, file_todate = self.select_file(security_id, fromdate, todate, path)
            if file_cached:
                _upsert(security_id, fromdate, todate)
                return True, file_fromdate, file_todate
            else:
                datafile = path + security_id + '.' + str(fromdate) + '.' + str(todate) + '.csv'
                self.log.debug('SEGUE download sul file <{}>'.format(datafile))
                c=subprocess.call(['../yahoodownload.py',
                                 '--ticker', security_id, \
                                 '--fromdate', str(fromdate), \
                                 '--todate', str(todate), \
                                 '--outfile', datafile])            #+#
                if c != 0:
                    #raise DownloadFailException('fail to download data for security {}'.format(security_id))
                    self.log.warning('FAIL to download data for security {}'.format(security_id))
                    return False
                else:
                    _upsert(security_id, fromdate, todate)
                    return True, str(fromdate), str(todate)


    def close(self):
        try:
            self.conn.close()
            self.log.info('syncdb <' + self.db_file + '> CLOSED')
        except Exception as e:
            raise e

