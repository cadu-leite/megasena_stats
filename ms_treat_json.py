'''


'''
import json
import sqlite3
from datetime import datetime


DATABASE_PATH = 'async_records.db'

INSERT_JOGO = '''INSERT  INTO JOGO (id, data, dezenas, acumulado, gdores_f1, gdores_f2, gdores_f3, uf, municipio) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''
INSERT_JOGO_ERROr = '''INSERT  INTO JOGO (id) VALUES (?)'''
SELECT_RESPOSES = '''SELECT id, date_add, date_upd, url, status, json FROM url_access ORDER BY id'''


def datebr_to_iso(datebr: str) -> str:
    '''
    conversao simples da string de data para ums string da data no formato iso
    '''

    print(f'\n\n PRINT - datebr=|{datebr}|\n')
    d = datetime.strptime(datebr, '%d/%m/%Y')
    return  d.strftime('%Y-%m-%d')

def clean_munic_uf(municipiouf: str) -> tuple:
    '''
    trata o campo do jsons
        ex.:
            "nomeMunicipioUFSorteio": "Brasília, DF",
    '''
    municipio = ''
    uf = ''
    try:
        municipio, uf = municipiouf.split(',')
    except:
        pass

    return tuple((uf, municipio))


def create_registro_jogo(sql_conn, datadict: dict) -> None:

    # sql_conn = sqlite3.connect(DATABASE_PATH)
    cur_ins = sql_conn.cursor()

    valid = True

    minimum_keys = ('numero', 'dataApuracao', 'listaDezenas', 'acumulado', 'listaRateioPremio', 'nomeMunicipioUFSorteio')

    if all(key in datadict for key in minimum_keys):

        # tupla de resultados
        values = (
            datadict['numero'],
            datebr_to_iso(datadict['dataApuracao']),
            '|'.join(datadict['listaDezenas']),
            datadict['acumulado'],
            datadict['listaRateioPremio'][0]['numeroDeGanhadores'],
            datadict['listaRateioPremio'][1]['numeroDeGanhadores'],
            datadict['listaRateioPremio'][2]['numeroDeGanhadores'],
            clean_munic_uf(datadict['nomeMunicipioUFSorteio'])[0],
            clean_munic_uf(datadict['nomeMunicipioUFSorteio'])[1],
        )

        # prsiste no BD
        cur_ins.execute(INSERT_JOGO, values)
        sql_conn.commit()

    else:
        valid = False

    cur_ins.close()
    # sql_conn.close()
    return valid


def main():

    sql_conn = sqlite3.connect(DATABASE_PATH)
    sql_conn.row_factory = sqlite3.Row

    cur_sel = sql_conn.cursor()

    for row in cur_sel.execute(SELECT_RESPOSES):
        # passa o ID para logar o ID do jogo caso haja erro
        valid = create_registro_jogo(sql_conn, json.loads(row['return']))

        if not valid:
            print(f'!!! ERRO: Registro JSON sem as chaves mínimas - {row}')

    cur_sel.close()
    sql_conn.close()


if __name__ == "__main__":
    main()