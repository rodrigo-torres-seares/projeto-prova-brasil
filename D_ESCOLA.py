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

        df_escola['NO_LOCALIZACAO'] = df_escola['CD_LOCALIZACAO'] \
            .map(lambda x: 'Urbana' if x == 1 else 'Rural')

        df_escola['NO_DEPENDENCIA_ADM'] = df_escola['CD_DEPENDENCIA_ADM'] \
            .map(lambda x:
                 'Federal' if x == 1 else
                 'Estadual' if x == 2 else 'Municipal'
                 )
        sk_data = range(1, df_escola.shape[0] + 1)
        df_escola['SK_ESCOLA'] = pd.Series(
            data=sk_data,
            name='SK_ESCOLA'
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
