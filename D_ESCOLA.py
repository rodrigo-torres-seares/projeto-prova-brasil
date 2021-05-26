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
                                            +POSTGRES_PORT)

    conn_dw = create_connection_postgre(POSTGRES_ADDRESS,
                                        POSTGRES_DBNAME_DW,
                                        POSTGRES_USER,
                                        POSTGRES_PASS,
                                        POSTGRES_PORT)


    def extract_dim_escola(conn_input):
        print('extracting data...')
        df_stage_escolas = get_data_from_database(conn_input,
                                        'select \
                                            se."PK_COD_ENTIDADE", \
                                            se."NO_ENTIDADE", \
                                            se."ID_DEPENDENCIA_ADM", \
                                            se."ID_LOCALIZACAO" \
                                        from "STAGE_ESCOLAS" se;'
                                    )

        df_stage_result_aluno =  get_data_from_database(conn_input,
                                       'select \
                                            sra."ID_ESCOLA" , \
                                            sra."ID_LOCALIZACAO", \
                                            sra."ID_DEPENDENCIA_ADM" \
                                        from "STAGE_RESULTADO_ALUNO" sra ;'  
                                    )

        df_escola = pd.merge(df_stage_escolas, 
                                df_stage_result_aluno, 
                                left_on='PK_COD_ENTIDADE', 
                                right_on='ID_ESCOLA')
        
        return df_escola

    def treat_dim_escola(df_escola):
        print('treating data...')
        
        df_escola.rename(columns={
            'PK_COD_ENTIDADE':'CD_ESCOLA',
            'NO_ENTIDADE':'NO_ESCOLA',
            'ID_DEPENDENCIA_ADM_x':'CD_DEPENDENCIA_ADM',
            'ID_LOCALIZACAO_x':'CD_LOCALIZACAO'
        }, inplace=True)
        
        df_escola.pop('ID_LOCALIZACAO_y')
        df_escola.pop('ID_DEPENDENCIA_ADM_y')
        df_escola.pop('ID_ESCOLA')
        df_escola['NO_LOCALIZACAO'] = df_escola['CD_LOCALIZACAO'].map(
                                                                lambda x: 
                                                                'Urbana' 
                                                                if x == 1 else 
                                                                'Rural'
                                                                )
        
        df_escola['NO_DEPENDENCIA_ADM'] = df_escola['CD_DEPENDENCIA_ADM'].map(
                                                                lambda x: 
                                                                'Federal' 
                                                                if x == 1 else 
                                                                'Estadual'
                                                                if x == 2 else
                                                                'Municipal'
                                                                )
        return df_escola

    def load_dim_escola(df_escola, conn_output):
        fill_table(df_escola, conn_output, 'D_ESCOLA', 1305, 'SK_ESCOLA')

    def run_dim_escola(conn_input, conn_output):
        try:
            df_escola = (
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