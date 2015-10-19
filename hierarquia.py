# -*- coding: utf-8 -*-

import logging
log = logging.getLogger(__name__)

import pandas as pd


def transform_hierarquia(ks13, ksh3):
    log.info('Convertendo hierarquia de centros de custo')

    # Remove os centros de custo afim de manter somente as hierarquias
    # no dataframe
    ksh3 = ksh3.query('hierarquia not in @ks13.centro_custo')

    # Mantém somente os centros de custo que possuem hierarquia válida
    ks13 = ks13.query('hierarquia in @ksh3.hierarquia')

    # Join dos dois dataframes
    df = ksh3.merge(ks13,
                    left_on='hierarquia',
                    right_on='hierarquia',
                    how='inner')

    df['nivel'] += 1

    # Opção 1: Não usar a descrição do centro de custo
    # df = df.apply(preencher_nivel_responsavel, axis=1)

    # Opção 2: Usar descrição do centro de custo na coluna nível
    df = df.apply(preencher_nivel_descricao, axis=1)

    df.drop(['hierarquia', 'descricao_x', 'descricao_y', 'tipo_centro_custo'],
            axis=1, inplace=True)
    df.rename(columns={'centro_custo': 'hierarquia',
                       'responsavel': 'descricao'},
              inplace=True)

    # Neste momento, o dataframe df contém apenas as hierarquias que tem
    # um centro de custo associado.
    # Então, precisamos concatena o restante das hierarquias ao dataframe.
    hierarquias = ksh3.query('hierarquia not in @ks13.hierarquia')
    df = pd.concat([df, hierarquias], ignore_index=True)

    return df


def preencher_nivel_responsavel(row):
    nivel_hierarquia = 'nivel_{!s}_hierarquia'.format(row['nivel'])
    nivel_descricao = 'nivel_{!s}_descricao'.format(row['nivel'])

    row[nivel_hierarquia] = row['centro_custo']
    row[nivel_descricao] = row['responsavel']

    return row


def preencher_nivel_descricao(row):
    nivel_hierarquia = 'nivel_{!s}_hierarquia'.format(row['nivel'])
    nivel_descricao = 'nivel_{!s}_descricao'.format(row['nivel'])

    row[nivel_hierarquia] = row['centro_custo']
    row[nivel_descricao] = row['descricao_y']

    return row
