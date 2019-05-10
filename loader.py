#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

''' nuovo modulo che sustituisce i precedenti '''

import logging
import importlib

_adapters   = {}


def load_module(module_name):
    
    log = logging.getLogger(__name__)

    module = module_name.strip()
    log.debug('try to import module <{}>'.format(module))

    if module not in _adapters:
        try:
            _adapters[module] = importlib.import_module(module)
        except ImportError as e:
            raise e ### fail to load model
        else:
            log.debug('module <{}> succesfully imported'.format(module))
    
    return _adapters[module]
