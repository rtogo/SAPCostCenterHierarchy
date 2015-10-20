# -*- coding: utf-8 -*-

import logging
log = logging.getLogger(__name__)

import pandas as pd
import numpy as np
import os
import yaml


class ETL(object):
    def __init__(self, path, config_filename='de-para.yml'):
        self.path = path
        self.config_filename = config_filename
        self.df = pd.DataFrame()
        self.profile = pd.DataFrame()

        self.extract()
        self.transform()
        self.quality_check()

    def extract(self):
        self.df = pd.read_csv(self.path, encoding='latin-1', sep='\t',
                              header=0, dtype=str)

        self.df.columns = ['SAP']

        with open(self.config_filename, encoding='utf-8') as stream:
            self.config = yaml.load(stream)

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
        level_2 = None

        for i in self.df.index:
            row = self.df.loc[i].copy()
            last_row = self.df.shift(1).ix[i].copy()

            if row['nivel'] == 1 and row['descricao'] != level_1:
                level_1 = row['descricao']
                # log.info('    > Lendo hierarquia {!r}'.format(row['descricao']))

            if row['nivel'] == 2 and row['descricao'] != level_2:
                level_2 = row['descricao']
                log.info('    > Lendo hierarquia {!s} :: {!s}'.
                         format(level_1, level_2))

            nivel_hierarquia = 'nivel_{!s}_hierarquia'.format(row['nivel'])
            nivel_descricao = 'nivel_{!s}_descricao'.format(row['nivel'])
            self.df.loc[i, nivel_hierarquia] = row['hierarquia']
            self.df.loc[i, nivel_descricao] = row['descricao']

            for l in range(1, row['nivel']):
                nivel_hierarquia = 'nivel_{!s}_hierarquia'.format(l)
                nivel_descricao = 'nivel_{!s}_descricao'.format(l)

                self.df.loc[i, nivel_hierarquia] = last_row[nivel_hierarquia]
                self.df.loc[i, nivel_descricao] = last_row[nivel_descricao]

    def quality_check(self):
        pass


def parse(path):
    log.info('Importando ksh3')
    parser = ETL(path)

    return parser.df
