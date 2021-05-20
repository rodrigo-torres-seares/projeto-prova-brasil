import pandas as pd
import time
import CONEXAO as cnx

if __name__ == '__main__':
    
    def read_file(path, delimiter):    #leitura do arquivo
        return pd.read_csv(path,
                            sep=delimiter,
                            error_bad_lines=False, 
                            low_memory=True,
                            encoding='utf8')

    def corrigir_coluna_turno(df):    #corrigir dados missing para a conversão de tipo
        df['ID_TURNO'] = df['ID_TURNO'].map(lambda x: 0 if x == ' ' else x).astype('int64')

    def create_stage(data, sql_conn, table, action): #função para criar a stage no banco de dados   
        try:
            print('preenchendo tabela...')
            data.to_sql(table, con=sql_conn, if_exists=action)
        except Exception as e:
            print(e)

    def fill_stage(data_frame, table, frame_size):  #função para divivir o dataframe em pedados para preencher a stage
        size = data_frame.shape[0]
        frame = int(size/frame_size) 
        for i in range(0, size+1, frame):
            data = data_frame.loc[i:i+frame-1]
            create_stage(data, conn, table, 'append')
   
    POSTGRES_ADDRESS = '192.168.3.2'
    POSTGRES_PORT = '5432'
    POSTGRES_USER = 'itix'
    POSTGRES_PASS = 'itix123'
    POSTGRES_DBNAME = 'stage_prova_brasil'

    conn = cnx.create_connection_postgre(POSTGRES_ADDRESS, #conexão com o banco da stage
                                        POSTGRES_DBNAME, 
                                        POSTGRES_USER, 
                                        POSTGRES_PASS, 
                                        POSTGRES_PORT)

    start = time.time()
    df_escolas = read_file('Datasets_projeto/ESCOLAS.csv', '|')
    create_stage(df_escolas, conn, 'STAGE_ESCOLAS', 'replace')

    df_ibge = read_file('Datasets_projeto/DADOS_IBGE.csv', ';')
    create_stage(df_ibge, conn, 'STAGE_DADOS_IBGE', 'replace')

    df_resultado_aluno = read_file('Datasets_projeto/TS_RESULTADO_ALUNO.csv', ';')
    corrigir_coluna_turno(df_resultado_aluno)
    fill_stage(df_resultado_aluno, 'STAGE_RESULTADO_ALUNO', 45)
    exec_time = time.time() - start
    
    file = open('saida.txt', 'a')
    file.write('tempo de execução: {}'.format(exec_time))
    file.close()
    