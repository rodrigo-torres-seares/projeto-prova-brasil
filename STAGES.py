from numpy import index_exp
import pandas as pd

def read_file(path, delimiter):  # leitura do arquivo
    return pd.read_csv(path,
                       sep=delimiter,
                       error_bad_lines=False,
                       low_memory=True,
                       encoding='utf8')

def insert_data(data, connection, table, action):  # função para inserir dados no banco de dados
    data.to_sql(table, con=connection, if_exists=action, index=False)


def fill_table(data_frame, connection, table, frame_size):  # função que prenche uma tabela com seguimentos do dataframe
    try:
        print('filling table {}...'.format(table))
        size = data_frame.shape[0]
        frame = int(size/frame_size)
        for i in range(0, size+1, frame):
            data = data_frame.loc[i:i+frame-1]
            insert_data(data, connection, table, 'append')
    except Exception as e:
        print(e)