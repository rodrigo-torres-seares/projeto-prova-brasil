import time as t
import pandas as pd
from CONEXAO import create_connection_postgre
from IO_DATA import insert_data, get_data_from_database

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

    def extract_fato_prova(conn_stage, conn_dw):
        print('extracting data...')
        df_resultado_aluno = get_data_from_database(
            conn_stage,
            f'select sra."ID_PROVA_BRASIL", sra."IN_PREENCHIMENTO",\
            sra."ID_MUNICIPIO" , sra."ID_ESCOLA", sra."ID_TURMA",\
            sra."IN_PROFICIENCIA" , sra."PROFICIENCIA_LP_SAEB",\
            sra."PROFICIENCIA_MT_SAEB" \
            from "STAGE_RESULTADO_ALUNO" sra;'
        )

        df_localidade = get_data_from_database(
            conn_dw,
            f'select dl."SK_LOCALIDADE", dl."CD_MUNICIPIO"\
            from "D_LOCALIDADE" dl; '
        )

        df_turma = get_data_from_database(
            conn_dw,
            f'select dt."SK_TURMA", dt."CD_TURMA"\
            from "D_TURMA" dt; '
        )

        df_escola = get_data_from_database(
            conn_dw,
            f'select de."SK_ESCOLA", de."CD_ESCOLA" \
            from "D_ESCOLA" de ;'
        )

        df_resultado_aluno = pd.merge(
            df_resultado_aluno,
            df_localidade,
            left_on='ID_MUNICIPIO',
            right_on='CD_MUNICIPIO',
            how='inner'
        )

        df_resultado_aluno = pd.merge(
            df_resultado_aluno,
            df_turma,
            left_on='ID_TURMA',
            right_on='CD_TURMA',
            how='inner'
        )

        df_resultado_aluno = pd.merge(
            df_resultado_aluno,
            df_escola,
            left_on='ID_ESCOLA',
            right_on='CD_ESCOLA',
            how='inner'
        )

        return df_resultado_aluno


    def treat_fato_prova(df_resultado_aluno):
        print('treating data...')

        df_resultado_aluno.rename(columns={
            "ID_PROVA_BRASIL": "NU_ANO_PROVA",
            "IN_PREENCHIMENTO": "FL_PREENCHIMENTO",
            "IN_PROFICIENCIA": "FL_PROFICIENCIA",
            "PROFICIENCIA_LP_SAEB": "VL_PROFICIENCIA_LP_SAEB",
            "PROFICIENCIA_MT_SAEB": "VL_PROFICIENCIA_MT_SAEB"
        }, inplace=True)

        df_resultado_aluno.pop('ID_TURMA')
        df_resultado_aluno.pop('CD_TURMA')
        df_resultado_aluno.pop('ID_ESCOLA')
        df_resultado_aluno.pop('CD_ESCOLA')
        df_resultado_aluno.pop('ID_MUNICIPIO')
        df_resultado_aluno.pop('CD_MUNICIPIO')

        df_resultado_aluno['VL_PROFICIENCIA_LP_SAEB']\
            = df_resultado_aluno['VL_PROFICIENCIA_LP_SAEB'].map(
            lambda x: float(x.replace(",", ".")
                            if len(x) > 1 else -1)
        )
        df_resultado_aluno['VL_PROFICIENCIA_MT_SAEB'] \
            = df_resultado_aluno['VL_PROFICIENCIA_MT_SAEB'].map(
            lambda x: float(x.replace(",", ".")
                            if len(x) > 1 else -1)
        )

        d_missing = {
            'NU_ANO_PROVA': [-1],
            'FL_PREENCHIMENTO': [-1],
            'FL_PROFICIENCIA': [-1],
            'VL_PROFICIENCIA_LP_SAEB': [-1],
            'VL_PROFICIENCIA_MT_SAEB': [-1],
            'SK_LOCALIDADE': [-1],
            'SK_TURMA': [-1],
            'SK_ESCOLA': [-1]
        }

        d_not_applicable = {
            'NU_ANO_PROVA': [-2],
            'FL_PREENCHIMENTO': [-2],
            'FL_PROFICIENCIA': [-2],
            'VL_PROFICIENCIA_LP_SAEB': [-2],
            'VL_PROFICIENCIA_MT_SAEB': [-2],
            'SK_LOCALIDADE': [-2],
            'SK_TURMA': [-2],
            'SK_ESCOLA': [-2]
        }

        d_unknown = {
            'NU_ANO_PROVA': [-3],
            'FL_PREENCHIMENTO': [-3],
            'FL_PROFICIENCIA': [-3],
            'VL_PROFICIENCIA_LP_SAEB': [-3],
            'VL_PROFICIENCIA_MT_SAEB': [-3],
            'SK_LOCALIDADE': [-3],
            'SK_TURMA': [-3],
            'SK_ESCOLA': [-3]
        }

        df_missing = pd.DataFrame(data=d_missing)
        df_not_applicable = pd.DataFrame(data=d_not_applicable)
        df_unknown = pd.DataFrame(data=d_unknown)

        df_resultado_aluno = pd.concat(
            [df_missing, df_not_applicable, df_unknown, df_resultado_aluno],
            ignore_index=True
        )

        return df_resultado_aluno

    def load_fato_prova(df_resultado_aluno, conn_output):
        insert_data(
            df_resultado_aluno,
            conn_output,
            'F_PROVA',
            'replace',
            2000
        )

    def run_fato_prova(conn_input, conn_output):
        try:
            (
                extract_fato_prova(conn_input, conn_output)
                .pipe(treat_fato_prova)
                .pipe(load_fato_prova, conn_output)
            )
        except Exception as e:
            print(e)


    start = t.time()
    run_fato_prova(conn_stage, conn_dw)
    exec_time = t.time() - start
    print('exec time = {}'.format(exec_time))

