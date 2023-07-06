#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, re
#from pathlib import Path

class datasource():

    def __init__(self, db_dir=None, db_file=None, path=None, strict=False):

        self.strict = strict

        #db_dir = Path(db_dir)
        if os.path.isdir(db_dir) and db_file:
            self.db_dir =db_dir
            self.db_file=db_file
        else:
            self.log.error('invalid datasource path : <{}>'.format(self.db_dir + self.db_file))
            sys.exit(1)
