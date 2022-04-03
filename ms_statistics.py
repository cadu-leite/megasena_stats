'''


'''
import itertools
import sqlite3
from datetime import datetime
from collections import Counter

DATABASE_PATH = 'async_records.db'

# INSERT_JOGO = '''INSERT  INTO JOGO (id, data, dezenas, acumulado, gdores_f1, gdores_f2, gdores_f3, uf, municipio) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)'''
SELECT_JOGOS = '''SELECT dezenas FROM jogo '''


def main():

    sql_conn = sqlite3.connect(DATABASE_PATH)
    cur_sel = sql_conn.cursor()
    recs = cur_sel.execute(SELECT_JOGOS)
    recs = recs.fetchall()

    l = list(itertools.chain.from_iterable( [ x[0].split('|') for x in recs ]))
    z = Counter(l)

    odk = OrderedDict(sorted(z.items()))
    odi = dict(sorted(z.items(), key=lambda item: item[1]))

    cur_sel.close()
    sql_conn.close()


if __name__ == "__main__":
    main()