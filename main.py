import pandas as pd
import time
import CONEXAO as cnx
import STAGES as s

pd.options.display.float_format = '{:.2f}'.format

if __name__ == '__main__':
    POSTGRES_ADDRESS = '192.168.3.2'
    POSTGRES_PORT = '5432'
    POSTGRES_USER = 'itix'
    POSTGRES_PASS = 'itix123'
    POSTGRES_DBNAME = 'stage_prova_brasil'

    conn = cnx.create_connection_postgre(POSTGRES_ADDRESS,  # conexão com o banco das stages
                                         POSTGRES_DBNAME,
                                         POSTGRES_USER,
                                         POSTGRES_PASS,
                                         POSTGRES_PORT)

    # corrigir dados missing para a conversão de tipo
    def corrigir_coluna_turno(df):
        df['ID_TURNO'] = df['ID_TURNO'].map(lambda x:
                                            0 if x == ' ' else x).astype('int64')
    start = time.time()
    df_escolas = s.read_file('Datasets_projeto/ESCOLAS.csv', '|')
    s.create_stage(df_escolas, conn, 'STAGE_ESCOLAS', 'replace')

    df_ibge = s.read_file('Datasets_projeto/DADOS_IBGE.csv', ';')
    s.create_stage(df_ibge, conn, 'STAGE_DADOS_IBGE', 'replace')

    df_resultado_aluno = s.read_file('Datasets_projeto/TS_RESULTADO_ALUNO.csv', ';')
    corrigir_coluna_turno(df_resultado_aluno)
    s.fill_stage(df_resultado_aluno, conn, 'STAGE_RESULTADO_ALUNO', 45)
    exec_time = time.time() - start

    file = open('saida.txt', 'a')
    file.write('tempo de execução: {}'.format(exec_time))
    file.close()
