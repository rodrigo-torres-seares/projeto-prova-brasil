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
#Pq tem dois create connection ? 

    def extract_dim_localidade(conn_input):
        print('extracting data...')
        df_resultado_aluno = get_data_from_database(
            conn_input,
            f'select distinct sra."ID_UF", sra."ID_MUNICIPIO"\
            from "STAGE_RESULTADO_ALUNO" sra;'

        )
        #Pq vc buscou o dados de municípios e uf da stage resultado aluno e não utilizou?

               
        df_municipios = get_data_from_database(
            conn_input,
            f'select sdi."Cod.", sdi."Regiao"\
            from "STAGE_DADOS_IBGE" sdi\
            where sdi."Nivel" = '"'MU'"';'
        )
        

        df_ufs = get_data_from_database(
            conn_input,
            f'select sdi."Cod.", sdi."Regiao"\
            from "STAGE_DADOS_IBGE" sdi\
            where sdi."Nivel" = '"'UF'"';'
        )

        df_localidade = pd.merge(
            
            ,
            df_ufs,
            left_on='ID_UF',
            right_on='Cod.',
            how='inner'
        )

        df_localidade = pd.merge(
            df_localidade,
            df_municipios,
            left_on='ID_MUNICIPIO',
            right_on='Cod.',
            how='inner'
        )

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

        df_localidade['NO_MUNICIPIO'] = df_localidade['NO_MUNICIPIO'] \
            .apply(lambda x: x[:-5])

        sk_data = range(1, df_localidade.shape[0] + 1)
        df_localidade['SK_LOCALIDADE'] = pd.Series(
            data=sk_data,
            name='SK_LOCALIDADE'
        )

        d_missing = {
            'CD_UF': [-1],
            'NO_UF': ['Não informado'],
            'CD_MUNICIPIO': [-1],
            'NO_MUNICIPIO': ['Não informado'],
            'SK_LOCALIDADE': [-1]
        }

        d_not_applicable = {
            'CD_UF': [-2],
            'NO_UF': ['Não aplicável'],
            'CD_MUNICIPIO': [-2],
            'NO_MUNICIPIO': ['Não aplicável'],
            'SK_LOCALIDADE': [-2]
        }

        d_unknown = {
            'CD_UF': [-3],
            'NO_UF': ['Desconhecido'],
            'CD_MUNICIPIO': [-3],
            'NO_MUNICIPIO': ['Desconhecido'],
            'SK_LOCALIDADE': [-3]
        }

        df_missing = pd.DataFrame(data=d_missing)
        df_not_applicable = pd.DataFrame(data=d_not_applicable)
        df_unknown = pd.DataFrame(data=d_unknown)

        df_localidade = pd.concat(
            [df_missing, df_not_applicable, df_unknown, df_localidade],
            ignore_index=True
        )

        return df_localidade
    
     #Depois da uma olhada no meu código pra ver a forma que foi feito para podermos padronizar os proximos projetos 
    #Essa parte de tratamento podemos deixar mais "limpo" usando assign e Method Chaining
    #tem exemplos no meu código e nesse site https://tomaugspurger.github.io/method-chaining.html

    def load_dim_localidade(df_localidade, conn_output):
        insert_data(
            df_localidade,
            conn_output,
            'D_LOCALIDADE',
            'replace',
            2000
        )


    def run_dim_localidade(conn_input, conn_output):
        try:
            (
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
