import time as t
import pandas as pd
from CONEXAO import create_connection_postgre
from IO_DATA import insert_data, get_data_from_database

pd.options.display.float_format = '{:.2f}'.format

if __name__ == '__main__':
    POSTGRES_ADDRESS = '192.168.3.2'
    POSTGRES_PORT = '5432'
    POSTGRES_USER = 'itix'
    POSTGRES_PASS = 'itix123'
    POSTGRES_DBNAME_STAGE = 'stage_prova_brasil'
    POSTGRES_DBNAME_DW = 'dw_prova_brasil'

    conn_stage = create_connection_postgre(
        POSTGRES_ADDRESS,
        POSTGRES_DBNAME_STAGE,
        POSTGRES_USER,
        POSTGRES_PASS,
        POSTGRES_PORT
    )

    conn_dw = create_connection_postgre(
        POSTGRES_ADDRESS,
        POSTGRES_DBNAME_DW,
        POSTGRES_USER,
        POSTGRES_PASS,
        POSTGRES_PORT
    )

    def extract_dim_escola(conn_input):
        print('extracting data...')
        df_escola = get_data_from_database(
            conn_input,
            'select se."PK_COD_ENTIDADE", se."NO_ENTIDADE", \
                    se."ID_DEPENDENCIA_ADM", se."ID_LOCALIZACAO" \
            from "STAGE_ESCOLAS" se;'
        )

        return df_escola

    def treat_dim_escola(df_escola):
        print('treating data...')

        df_escola.rename(columns={
            'PK_COD_ENTIDADE': 'CD_ESCOLA',
            'NO_ENTIDADE': 'NO_ESCOLA',
            'ID_DEPENDENCIA_ADM': 'CD_DEPENDENCIA_ADM',
            'ID_LOCALIZACAO': 'CD_LOCALIZACAO'
        }, inplace=True)

        df_escola['DS_LOCALIZACAO'] = df_escola['CD_LOCALIZACAO'] \
            .map(lambda x: 'Urbana' if x == 1 else 'Rural')

        df_escola['DS_DEPENDENCIA_ADM'] = df_escola['CD_DEPENDENCIA_ADM'] \
            .map(lambda x:
                 'Federal' if x == 1 else
                 'Estadual' if x == 2 else 'Municipal'
                 )
        sk_data = range(1, df_escola.shape[0] + 1)
        df_escola['SK_ESCOLA'] = pd.Series(
            data=sk_data,
            name='SK_ESCOLA'
        )

        d_missing = {
            'CD_ESCOLA': [-1],
            'NO_ESCOLA': ['Não informado'],
            'CD_DEPENDENCIA_ADM': [-1],
            'DS_DEPENDENCIA_ADM': ['Não informado'],
            'CD_LOCALIZACAO': [-1],
            'DS_LOCALIZACAO': ['Não informado'],
            'SK_ESCOLA': [-1]
        }

        d_not_applicable = {
            'CD_ESCOLA': [-2],
            'NO_ESCOLA': ['Não aplicável'],
            'CD_DEPENDENCIA_ADM': [-2],
            'DS_DEPENDENCIA_ADM': ['Não aplicável'],
            'CD_LOCALIZACAO': [-2],
            'DS_LOCALIZACAO': ['Não aplicável'],
            'SK_ESCOLA': [-2]
        }

        d_unknown = {
            'CD_ESCOLA': [-3],
            'NO_ESCOLA': ['Desconhecido'],
            'CD_DEPENDENCIA_ADM': [-3],
            'DS_DEPENDENCIA_ADM': ['Desconhecido'],
            'CD_LOCALIZACAO': [-3],
            'DS_LOCALIZACAO': ['Desconhecido'],
            'SK_ESCOLA': [-3]
        }

        df_missing = pd.DataFrame(data=d_missing)
        df_not_applicable = pd.DataFrame(data=d_not_applicable)
        df_unknown = pd.DataFrame(data=d_unknown)
        df_escola = pd.concat(
            [df_missing, df_not_applicable, df_unknown, df_escola],
            ignore_index=True
        )

        return df_escola

    def load_dim_escola(df_escola, conn_output):
        insert_data(
            df_escola,
            conn_output,
            'D_ESCOLA',
            'replace',
            2000
        )

    def run_dim_escola(conn_input, conn_output):
        try:
            (
                extract_dim_escola(conn_input)
                .pipe(treat_dim_escola)
                .pipe(load_dim_escola, conn_output)
            )
        except Exception as e:
            print(e)

    start = t.time()
    run_dim_escola(conn_stage, conn_dw)
    exec_time = t.time() - start
    print('exec time = {}'.format(exec_time))
