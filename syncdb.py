#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, sqlite3
import logging, logging.config, configparser
import subprocess

class DownloadFailException(Exception):
    pass


class syncdb():

    def __init__(self, db_dir=None, db_file=None, strict=False):

        self.log = logging.getLogger (__name__)

        self.strict = strict

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


    def select_file(self, _struct, f=None):
        # TOBE
        # verifica se f.fromdate <= _struct[fromdate] and f.todate>=_struct[todate]
        if not os.path.isfile(f):
            return False, None, None
        else:
            # TBD
            file_fromdate = '2017-06-01' # da estrarre da f (f.fromdate)    TODO
            file_todate = '2017-12-31' # da estrarre da f (f.todate)        TODO
            return True, file_fromdate, file_todate


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


    def insert_security_ex(self, _struct, security_id, fromdate, todate, datafile=None):

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
               file_cached, file_fromdate, file_todate = self.select_file(_struct, f=datafile)
               if file_cached:
                   _update(security_id, fromdate, todate)
                   return True
               else:
                   c=subprocess.call(['../yahoodownload.py',
                                    '--ticker', security_id, \
                                    '--fromdate', fromdate, \
                                    '--todate', todate, \
                                    '--outfile', datafile])            #+#
                   if c != 0:
                       self.log.warning('FAIL to download data for security {}'.format(security_id))
                       return False
                   else:
                       _upsert(security_id, fromdate, todate)
                       return True
           else:
               self.log.warning('WARNING : unknow security <{}>'.format(security_id))
               return False
        else:
            # block (upsert)
            file_cached, file_fromdate, file_todate = self.select_file(_struct, f=datafile)
            if file_cached:
                _upsert(security_id, fromdate, todate)
                return True
            else:
                c=subprocess.call(['../yahoodownload.py',
                                 '--ticker', security_id, \
                                 '--fromdate', fromdate, \
                                 '--todate', todate, \
                                 '--outfile', datafile])            #+#
                if c != 0:
                    #raise DownloadFailException('fail to download data for security {}'.format(security_id))
                    self.log.warning('FAIL to download data for security {}'.format(security_id))
                    return False
                else:
                    _upsert(security_id, fromdate, todate)
                    return True

        '''
        # block
        file_cached, file_fromdate, file_todate = self.select_file(_struct, f=datafile)
        if file_cached:
            upsert(security_id, fromdate, todate)
            return
        else:
            c=subprocess.call(['../yahoodownload.py',
                             '--ticker', security_id, \
                             '--fromdate', fromdate, \
                             '--todate', todate, \
                             '--outfile', datafile])            #+#
            if c != 0:
                raise DownloadFailException('fail to download data for security {}'.format(security_id))
            else:
                upsert(security_id, fromdate, todate)
                return
        #else:
        #    self.log.warning('ERROR : unknow security <{}>'.format(security_id))
        #    sys.exit(1)
        '''

    def close(self):
        try:
            self.conn.close()
            self.log.info('syncdb <' + self.db_file + '> CLOSED')
        except Exception as e:
            raise e

