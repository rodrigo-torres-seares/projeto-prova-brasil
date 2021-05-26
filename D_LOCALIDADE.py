import time as t
import pandas as pd
from CONEXAO import create_connection_postgre
from IO_DATA import fill_table, get_data_from_database

pd.options.display.float_format = '{:.2f}'.format

if __name__ == '__main__':
    POSTGRES_ADDRESS = '192.168.3.2'
    POSTGRES_PORT = '5432'
    POSTGRES_USER = 'itix'
    POSTGRES_PASS = 'itix123'
    POSTGRES_DBNAME_STAGE = 'stage_prova_brasil'
    POSTGRES_DBNAME_DW = 'dw_prova_brasil'

    conn_stage = create_connection_postgre(POSTGRES_ADDRESS,
                                            POSTGRES_DBNAME_STAGE,
                                           POSTGRES_USER,
                                           POSTGRES_PASS,
                                           POSTGRES_PORT)

    conn_dw = create_connection_postgre(POSTGRES_ADDRESS,
                                        POSTGRES_DBNAME_DW,
                                        POSTGRES_USER,
                                        POSTGRES_PASS,
                                        POSTGRES_PORT)


    def extract_dim_localidade(conn_input):
        print('extracting data...')
        df_cod_municipio_uf = get_data_from_database(conn_input,
                                       'select \
                                            sra."ID_UF", \
                                            sra."ID_MUNICIPIO" \
                                        from "STAGE_RESULTADO_ALUNO" sra ;'
                                    )

        df_municipios =  get_data_from_database(conn_input,
                                       'select \
                                            sdi."Cod.",\
                                            sdi."Regiao"\
                                        from "STAGE_DADOS_IBGE" sdi\
	                                    where sdi."Nivel" = '+ "'MU'" + ';'  
                                    )
        
        df_uf =  get_data_from_database(conn_input,
                                       'select \
                                            sdi."Cod.",\
                                            sdi."Regiao"\
                                        from "STAGE_DADOS_IBGE" sdi\
	                                    where sdi."Nivel" = ' + "'UF'" + ';'  
                                    )

        df_localidade = pd.merge(df_cod_municipio_uf, 
                                df_uf, 
                                left_on='ID_UF', 
                                right_on='Cod.')
        df_localidade = pd.merge(df_localidade, 
                                df_municipios, 
                                left_on='ID_MUNICIPIO', 
                                right_on='Cod.')
        return df_localidade

    def treat_dim_localidade(df_localidade):
        print('treating data...')
        
        df_localidade.rename(columns={
            'ID_UF': 'CD_UF',
            'ID_MUNICIPIO': 'CD_MUNICIPIO',
            'Regiao_x': 'NO_UF',
            'Regiao_y': 'NO_MUNICIPIO'
        }, inplace=True)
        
        df_localidade.pop('Cod._x')
        df_localidade.pop('Cod._y')
        
        df_localidade['NO_MUNICIPIO'] = df_localidade['NO_MUNICIPIO'].apply(lambda x: x[:-5])
        
        return df_localidade

    def load_dim_localidade(df_localidade, conn_output):
        fill_table(df_localidade, conn_output, 'D_LOCALIDADE', 1305, 'SK_LOCALIDADE')

    def run_dim_localidade(conn_input, conn_output):
        try:
            df_localidade = (
                extract_dim_localidade(conn_input)
                .pipe(treat_dim_localidade)
                .pipe(load_dim_localidade, conn_output)
            )
        except Exception as e:
            print(e)

    start = t.time()
    run_dim_localidade(conn_stage, conn_dw)
    exec_time = t.time() - start
    print('exec time = {}'.format(exec_time))
