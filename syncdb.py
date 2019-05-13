#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sqlite3
import logging, logging.config, configparser
import subprocess

class DownloadFailException(Exception):
    pass


class syncdb():

    def __init__(self, db_dir=None, db_file=None):

        self.log = logging.getLogger (__name__)

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

        self.log.info ('create_default_schema() ....')
        schema_file = self.db_dir + 'default_syncdb_schema.sql'
        try:
            with open(schema_file, 'rt') as f:
                schema = f.read()
                self.conn.executescript(schema)
        
        except FileNotFoundError as e: # [Errno 2] No such schema 
            self.conn.close()
            #raise NoSqlFileFound   (custom exception TBD)
            raise e

        except Exception as e: # TBD errori sql nello script
            self.conn.close()
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

            print(str(_securities))

        except Exception as e:
            raise e

        return _securities 


    def insert_security(self, security_id, fromdate, todate, datafile=None):
        #
        # rendo transazionale : download + insert/update
        #
        # https://community.backtrader.com/topic/499/saving-datafeeds-to-csv/2
        
        c=subprocess.call(['../yahoodownload.py',
                         '--ticker', security_id, \
                         '--fromdate', fromdate, \
                         '--todate', todate, \
                         '--outfile', datafile])            #+#
        if c != 0:
            raise DownloadFailException

        self.log.info('security <' + security_id + '> download complete('+str(c)+')')
       
        # TBD : potrei fare if select then update else insert
        try:
            sql=f"insert into securities (code, start_date, end_date) values (:security_id , :fromdate, :todate)"
            self.conn.execute(sql, {'security_id':security_id, 'fromdate':fromdate, 'todate':todate})
            self.conn.commit()
        except sqlite3.IntegrityError as e:
            sql=f"update securities set start_date=:fromdate, end_date=:todate where code=:security_id"
            self.conn.execute(sql, {'security_id':security_id, 'fromdate':fromdate, 'todate':todate})
            self.conn.commit()
        except sqlite3.OperationalError as e:
            raise e
            
        self.log.info('security <' + security_id + '> succesfully added to syncdb')


    def close(self):
        try:
            self.conn.close()
            self.log.info('syncdb <' + self.db_file + '> CLOSED')
        except Exception as e:
            raise e

