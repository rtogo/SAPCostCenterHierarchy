# -*- coding: utf-8 -*-

import logging
log = logging.getLogger(__name__)

import pandas as pd
import os


class ETL(object):
    def __init__(self, path):
        self.path = path
        self.df = pd.DataFrame()
        self.profile = pd.DataFrame()

        self.extract()
        self.transform()
        self.quality_check()

    def extract(self):
        self.df = pd.read_csv(self.path, encoding='latin-1', sep='\t',
                              header=3, dtype=str)

        # Remove the un-interesting columns
        columns = ['Centro cst', 'Responsável', 'Descrição',
                   'TpC.', 'Hierq.std.']

        for c in self.df.columns:
            if c not in columns:
                self.df.drop(c, axis=1, inplace=True)

    def transform(self):
        # Rename columns
        self.df.rename(columns={'Centro cst': 'centro_custo',
                                'Responsável': 'responsavel',
                                'Descrição': 'descricao',
                                'TpC.': 'tipo_centro_custo',
                                'Hierq.std.': 'hierarquia'},
                       inplace=True)

    def quality_check(self):
        # centro_custo must not be null
        nulls = len(self.df.loc[self.df['centro_custo'].isnull()])
        if nulls > 0:
            log.error('{:d} Centro(s) de custo em branco'.format(nulls))
            raise ValueError('Centro de custo em branco')

        # hierarquia must not be null
        nulls = len(self.df.loc[self.df['hierarquia'].isnull()])
        if nulls > 0:
            log.error('{:d} Hierarquia(s) de custo em branco'.format(nulls))
            raise ValueError('Hierarquia em branco')


def parse(path):
    log.info('Importando ks13')
    parser = ETL(path)

    return parser.df
