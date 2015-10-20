# -*- coding: utf-8 -*-

import logging
log = logging.getLogger(__name__)

import pandas as pd


def transform_hierarquia(df_ks13, df_ksh3):
    log.info('Convertendo hierarquia de centros de custo')

    # Remove os centros de custo afim de manter somente as hierarquias
    # no dataframe
    df_ksh3 = df_ksh3.query('@df_ksh3.hierarquia not in @df_ks13.centro_custo')

    # Mantém somente os centros de custo que possuem hierarquia válida
    df_ks13 = df_ks13.query('@df_ks13.hierarquia in @df_ksh3.hierarquia')

    # Join dos dois dataframes
    df = df_ksh3.merge(df_ks13,
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
    df_hierarquias = df_ksh3.query('@df_ksh3.hierarquia '
                                   'not in @df_ks13.hierarquia')
    df = pd.concat([df, df_hierarquias], ignore_index=True)

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
