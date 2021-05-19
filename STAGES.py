import pandas as pd
import time
import CONEXAO as cnx

if __name__ == '__main__':
    POSTGRES_ADDRESS = '192.168.3.2'
    POSTGRES_PORT = '5432'
    POSTGRES_USER = 'itix'
    POSTGRES_PASS = 'itix123'
    POSTGRES_DBNAME = 'stage_prova_brasil'
    file_path = 'Datasets_projeto/TS_RESULTADO_ALUNO.csv'
    
    conn = cnx.create_connection_postgre(POSTGRES_ADDRESS, #conexão com o banco da stage
                                    POSTGRES_DBNAME, 
                                    POSTGRES_USER, 
                                    POSTGRES_PASS, 
                                    POSTGRES_PORT)


    def corrigir_coluna_turno():    #corrigir dados missing para a conversão de tipo
        df['ID_TURNO'] = df['ID_TURNO'].map(lambda x: 0 if x == ' ' else x).astype('int64')

    def create_stage(data, sql_conn, table): #função para criar a stage no banco de dados   
        try:
            print('preenchendo tabela...')
            data.to_sql(table, con=sql_conn, if_exists='replace')
        except Exception as e:
            print(e)

    def fill_stage(data_frame, table):  #função para divivir o dataframe em pedados para preencher a stage
        size = df.shape[0]
        frame = int(size/90)
        for i in range(0, size, frame):
            data = data_frame.loc[i:i+frame]
            create_stage(data, conn, table)
   
    def read_file(path):
        return pd.read_csv(path,
                            sep=';',
                            error_bad_lines=False, 
                            low_memory=True)
    def create_stage_localidade():
        df_localidade = df[['ID_UF','ID_MUNICIPIO']]
        create_stage(df_localidade, conn, 'STAGE_LOCALIDADE')
    
    def create_stage_turma():
        df_turma = df[['ID_TURMA', 'ID_TURNO', 'ID_SERIE']]
        create_stage(df_turma, conn, 'STAGE_TURMA')
    
    def create_stage_escola():
        df_escola = df(['ID_ESCOLA', 'ID_LOCALIZACAO', 'ID_DEPENDENCIA_ADM'])


    start = time.time()
    df = read_file(file_path)
    corrigir_coluna_turno()
    #create_stage_localidade()
    create_stage_turma()
    exec_time = time.time() - start
    
    file = open('saida.txt', 'w')
    file.write('tempo de execução: {}'.format(exec_time))
    file.close()
    