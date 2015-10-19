# -*- coding: utf-8 -*-

import logging
log = logging.getLogger(__name__)

import pandas as pd
import numpy as np
import os
import yaml


class ETL(object):
    def __init__(self, path, config='config.yml'):
        self.path = path
        self.df = pd.DataFrame()
        self.profile = pd.DataFrame()

        with open(config, encoding='utf-8') as stream:
            self.config = yaml.load(stream)

        self.extract()
        self.transform()
        self.quality_check()

    def extract(self):
        self.df = pd.read_csv(self.path, encoding='latin-1', sep='\t',
                              header=0, dtype=str)

        self.df.columns = ['SAP']

    def transform(self):
        # Extrai colunas usando regex
        self.df[['nivel', 'hierarquia', 'descricao']] = \
            self.df.SAP.str.extract('^([\s\-|]+)([\w\d\-]+)(\s{6}.+$|$)')
        self.df.drop('SAP', axis=1, inplace=True)

        # Remove linhas onde qualquer campo for null
        self.df.dropna(axis=0, how='any', inplace=True)

        # Remove espaços excedentes
        self.df.descricao = self.df.descricao.apply(lambda x: x.strip())
        self.df.descricao = self.df.descricao.apply(
            lambda x: np.nan if x == '' else x)

        # Remove as linhas sem descrição
        self.df.dropna(subset=['descricao'], how='any', inplace=True)

        # Conta os pipes para saber o nível de cada hierarquia
        self.df.nivel = self.df.nivel.apply(
            lambda x: str(x).count('|')).astype(np.int16)

        # Reseta index para ficar com um número sequencial
        self.df.reset_index(drop=True, inplace=True)

        # Renomeia hierarquias conforme de->para no arquivo de configuração
        for k, v in self.config['Hierarquia de centros de custo'].items():
            self.df.loc[self.df.hierarquia == k, 'descricao'] = v

        # Denormaliza hierarquia
        level_1 = None
        for i in self.df.index:
            row = self.df.loc[i]

            if row['nivel'] == 1 and row['descricao'] != level_1:
                level_1 = row['descricao']
                log.info('    > Denormalizando {!r}'.format(row['descricao']))

            field_name = 'nivel_{!s}'.format(row['nivel'])
            self.df.loc[i, field_name] = row['hierarquia']

            if row['nivel'] > 1:
                for l in range(1, row['nivel']):
                    field_name = 'nivel_{!s}'.format(l)
                    self.df.loc[i, field_name] =\
                        self.df[field_name].shift(1).ix[i]

        print(self.df.head(50))

    def quality_check(self):
        pass


def parse(data_sources):
    log.info('Importando ksh3')
    path = os.path.join(data_sources['ksh3']['pasta'],
                        data_sources['ksh3']['arquivo'])
    parser = ETL(path)

    return parser.df
