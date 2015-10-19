# -*- coding: utf-8 -*-

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

import os
import ks13
import ksh3
import hierarquia

if __name__ == '__main__':
    import pandas as pd
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_columns', 500)

    data_sources = {
        'ks13': {'pasta': os.path.dirname(__file__), 'arquivo': 'ks13.txt'},
        'ksh3': {'pasta': os.path.dirname(__file__), 'arquivo': 'ksh3.txt'}
    }

    ks13 = ks13.parse(data_sources)
    ksh3 = ksh3.parse(data_sources)
    hierarquia = hierarquia.transform_hierarquia(ks13, ksh3)
    # print(ksh3)
