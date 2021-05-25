import pandas as pd

def read_file(path, delimiter):  # leitura do arquivo
    return pd.read_csv(path,
                       sep=delimiter,
                       error_bad_lines=False,
                       low_memory=True,
                       encoding='utf8')

def create_stage(data, connection, table, action):  # função para criar a stage no banco de dados
    try:
        print('filling table {}...'.format(table))
        data.to_sql(table, con=connection, if_exists=action)
    except Exception as e:
        print(e)

def fill_table(data_frame, connection, table, frame_size):  # função que prenche uma tabela com seguimentos do dataframe
    size = data_frame.shape[0]
    frame = int(size/frame_size)
    for i in range(0, size+1, frame):
        data = data_frame.loc[i:i+frame-1]
        create_stage(data, connection, table, 'append')
