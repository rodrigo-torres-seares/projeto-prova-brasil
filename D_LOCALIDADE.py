from os import cpu_count
import time as t
import pandas as pd
import CONEXAO as cnx
from STAGES import fill_table

pd.options.display.float_format = '{:.2f}'.format

if __name__ == '__main__':
    POSTGRES_ADDRESS_STAGE = '192.168.3.2'
    POSTGRES_PORT_STAGE = '5432'
    POSTGRES_USER_STAGE = 'itix'
    POSTGRES_PASS_STAGE = 'itix123'
    POSTGRES_DBNAME_STAGE = 'stage_prova_brasil'

    POSTGRES_ADDRESS_DW = '192.168.3.2'
    POSTGRES_PORT_DW = '5432'
    POSTGRES_USER_DW = 'itix'
    POSTGRES_PASS_DW = 'itix123'
    POSTGRES_DBNAME_DW = 'dw_prova_brasil'

    conn_stage = cnx.create_connection_postgre(POSTGRES_ADDRESS_STAGE,
                                            POSTGRES_DBNAME_STAGE,
                                            POSTGRES_USER_STAGE,
                                            POSTGRES_PASS_STAGE,
                                            POSTGRES_PORT_STAGE)

    conn_dw = cnx.create_connection_postgre(POSTGRES_ADDRESS_DW,
                                            POSTGRES_DBNAME_DW,
                                            POSTGRES_USER_DW,
                                            POSTGRES_PASS_DW,
                                            POSTGRES_PORT_DW)

    def get_data_from_database(conn_stage, sql_query):
        return pd.read_sql_query(sql=sql_query, con=conn_stage)

    def extract_dim_localidade(conn_input):
        return get_data_from_database(conn_stage,
                                        'select sra."ID_UF", \
                                        sra."ID_MUNICIPIO" from\
                                        "STAGE_RESULTADO_ALUNO" sra ;'
                                        )

    def treat_dim_localidade(df_localidade):
        df_localidade.rename(columns={
                                    'ID_UF': 'CD_UF',
                                    'ID_MUNICIPIO': 'CD_MUNICIPIO'
                                    },inplace=True)
        return df_localidade
        
    def load_dim_localidade(df_localidade, conn_output):
        fill_table(df_localidade, conn_output, 'D_LOCALIDADE', 45)
    
    def run_dim_localidade(conn_input ,conn_output):
        df_localidade = (
                        extract_dim_localidade(conn_input)
                        .pipe(treat_dim_localidade)
                        .pipe(load_dim_localidade, conn_output)
        )

    def resume_dataframe(dataframe):
        print('\ncaracterísticas do dataframe:\n')
        print('colunas:\n', dataframe.columns)
        print('tipo de dados das colunas:\n',dataframe.dtypes)
        print('dimensões do dataframe:\n', dataframe.shape)
        print('há dados missing:\n', dataframe.isna().any().any())
        print('quantidadee de dados missing:\n', dataframe.isna().sum().sum())
        print('há dados nulos:\n', dataframe.isnull().any().any())
        print('quantidadee de dados nulos:\n', dataframe.isnull().sum().sum())
        print('descrição estatística:\n', dataframe.describe())

    start = t.time() 
    run_dim_localidade(conn_stage, conn_dw)
    exec_time = t.time() - start
    print('exec time = {}'.format(exec_time))

    
