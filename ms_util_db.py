import sqlite3


def valid_text_json(text_json: str) -> bool:
    '''
    Aparentemente o servico em https://servicebus2.caixa.gov.br/portaldeloterias/api/megasena/
    pode retornar um JSON sempre, mesmo que este json venha com erro (ex.: jogo inexistente, ou por algum outro motivo)
    Ex.:
        ```json
            {
              "exceptionMessage": null,
              "innerMessage": null,
              "message": "Ocorreu um erro inesperado.",
              "stackTrace": null
            }
        ```
    Esta funcao verifica o registro depois de criado se o JSON tem de fato as informações completas do jogo.
    '''

    minimum_keys = ('numero', 'dataApuracao', 'listaDezenas')

    return all (word in text_json for word in minimum_keys)


def mark_bad_json(database_path):
    '''

    UPDATE  url_access SET json_valid=0 WHERE json  LIKE "%exceptionMessage%"

    '''
    conn = sqlite3.connect(database_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(r'UPDATE  url_access SET json_valid=0 WHERE json  LIKE "%exceptionMessage%"')

    conn.commit()

    recs = cur.fetchall()

    cur.close()
    conn.close()

    return recs


def get_recs_bad_json(database_path):
    '''

    UPDATE  url_access SET json_valid=0 WHERE json  LIKE "%exceptionMessage%"

    '''
    recs = select_dict(database_path, 'SELECT * FROM url_access WHERE json_valid = 0')

    return recs

def select_dict(database_path: str, select_query: str) -> list:
    '''
    Recebe uma QUERY "SELECT" somente,
    e retorna uma lista de dicionários representando os registros
    cada chamada uma conexão com o DB
    '''
    conn = sqlite3.connect(database_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    for row in cur.execute(select_query):
        yield(row)

    cur.close()
    conn.close()



def main():

    for row in select_dict(DATABASE_PATH, "select * from url_access"):
        valid_text_json(row['response'])



if __name__ == "__main__":
    main()