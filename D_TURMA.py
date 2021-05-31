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


    def extract_dim_turma(conn_input):
        print('extracting data...')
        df_turma = get_data_from_database(
            conn_input,
            f'select distinct sra."ID_TURMA",\
            sra."ID_TURNO" , sra."ID_SERIE" \
            from "STAGE_RESULTADO_ALUNO" sra;'
        )

        return df_turma

    def treat_dim_turma(df_turma):
        print('treating data...')

        df_turma.rename(columns={
            'ID_TURMA': 'CD_TURMA',
            'ID_TURNO': 'CD_TURNO',
            'ID_SERIE': 'CD_SERIE',
        }, inplace=True)

        df_turma['CD_TURNO'] = df_turma['CD_TURNO'] \
            .map(lambda x: int(x) if x.isnumeric() else -1)

        df_turma['DS_TURNO'] = df_turma['CD_TURNO'] \
            .map(lambda x:
                 'Matutino' if x == 1 else
                 'Vespertino' if x == 2 else
                 'Noturno' if x == 3 else
                 'Intermediário' if x == 4 else
                 'Não informado'
                 )

        df_turma['DS_SERIE'] = df_turma['CD_SERIE'] \
            .map(lambda x:
                 '4ª série/5º ano EF'
                 if x == 5 else
                 '8ª série/9º ano EF'
                 if x == 9 else
                 'Não informado'
                 )
        sk_data = range(1, df_turma.shape[0] + 1)
        df_turma['SK_TURMA'] = pd.Series(
            data=sk_data,
            name='SK_TURMA'
        )

        return df_turma


    def load_dim_turma(df_turma, conn_output):
        insert_data(
            df_turma,
            conn_output,
            'D_TURMA',
            'replace',
            2000
        )


    def run_dim_turma(conn_input, conn_output):
        try:
            (
                extract_dim_turma(conn_input)
                .pipe(treat_dim_turma)
                .pipe(load_dim_turma, conn_output)

            )
        except Exception as e:
            print(e)


    start = t.time()
    run_dim_turma(conn_stage, conn_dw)
    exec_time = t.time() - start
    print('exec time = {}'.format(exec_time))
