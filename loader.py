#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

import logging
from importFromURI import importModule 

###@starq69: MVC

__all__ = ['load_adapter']

_adapters = {}

def load_adapter(conf, module_name):
    '''
    conf : non utilizzato
    '''
    log = logging.getLogger(__name__)

    module = module_name.strip()
    log.debug('try to import module <{}>'.format(module))

    if module not in _adapters:
        try:
            _adapters[module] = importModule(module)

        except Exception as e:
            raise e ### fail to load model
    
    return _adapters[module]

