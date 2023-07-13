#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, re, logging

from pathlib import Path
from pathvalidate import is_valid_filename #https://kodify.net/python/validate-check-filename/


class datasource():

    def __init__(self, db_dir=None, db_file=None, path=None, strict=False):

        self.log        = logging.getLogger (__name__)
        self.strict     = strict
        self.path       = path #starq@TODO necessario?

        '''
        self.db_dir     = ''
        self.db_file    = ''
        self.path       = ''
        '''
        #self.log.debug('db_dir : <{}>, db_file : <{}>'.format(db_dir, db_file))
        self.log.debug(f'db_dir : <{db_dir}>, db_file : <{db_file}>')

        #db_dir = Path(db_dir)
        if db_dir and os.path.isdir(db_dir) : #and is_valid_filename(db_file, platform="universal"):
            self.db_dir =db_dir
            self.db_file=db_file
        else:
            #self.log.error('invalid datasource path : <{}>'.format(db_dir))
            self.log.error(f'invalid datasource path : <{db_dir}>')
            sys.exit(1)

        if is_valid_filename(db_file, platform="universal"):
            self.db_file=db_file
        else:
            #self.log.error('invalid db_file : <{}>'.format(db_file))
            self.log.error(f'invalid db_file : <{db_file}>')
            sys.exit(1)
