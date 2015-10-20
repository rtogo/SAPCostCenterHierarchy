# -*- coding: utf-8 -*-

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

import os
import sqlalchemy

import ks13
import ksh3
import hierarquia


def parse(source_ks13, source_ksh3, engine):
    df_ks13 = ks13.parse(source_ks13)
    df_ksh3 = ksh3.parse(source_ksh3)
    df_hierarquia = hierarquia.transform_hierarquia(df_ks13, df_ksh3)

    log.info('Salvando hierarquia')
    # df_ks13.to_sql('ks13', engine, if_exists='replace', index=False)
    # df_ksh3.to_sql('ksh3', engine, if_exists='replace', index=False)
    df_hierarquia.to_sql('hierarquia-centros-custo',
                         engine, if_exists='replace', index=False)


if __name__ == '__main__':
    import pandas as pd
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_columns', 500)

    engine = sqlalchemy.create_engine('sqlite:///hierarquia-centro-custo.db')
    ks13_filename = os.path.join(os.path.dirname(__file__), 'ks13.txt')
    ksh3_filename = os.path.join(os.path.dirname(__file__), 'ksh3.txt')

    parse(ks13_filename, ksh3_filename, engine)
