# -*- coding: utf-8 -*-

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

import os
import ks13
import ksh3

if __name__ == '__main__':
    data_sources = {
        'ks13': {'pasta': os.path.dirname(__file__), 'arquivo': 'ks13.txt'},
        'ksh3': {'pasta': os.path.dirname(__file__), 'arquivo': 'ksh3.txt'}
    }

    ks13 = ks13.parse(data_sources)
    ksh3 = ksh3.parse(data_sources)
    # print(ksh3)
