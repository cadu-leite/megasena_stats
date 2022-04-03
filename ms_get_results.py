'''

URL do servico da MegaSena - https://servicebus2.caixa.gov.br/portaldeloterias/api/megasena/


Estrutura tabela
================

CREATE TABLE "url_access" (
    "id"        INTEGER NOT NULL UNIQUE,
    "date_add"  timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "date_upd"  timestamp,
    "url"       TEXT UNIQUE,
    "status"    INTEGER DEFAULT 0,
    "json"    TEXT,
    "json_valid"    INTEGER DEFAULT 0,
    PRIMARY KEY("id" AUTOINCREMENT)
)

    INSERT INTO url_access (url, status, json) VALUES ('url/test', 200, '{<json>}');
    INSERT INTO url_access (url, status, json) VALUES (?, ?, ?);
    SELECT * FORM url_access

Limpa DB
---------

    delete from url_access;
    update sqlite_sequence  set  seq=1 where name='url_access';

);


ERRo no JSON
------------

```json
    {
      "exceptionMessage": null,
      "innerMessage": null,
      "message": "Ocorreu um erro inesperado.",
      "stackTrace": null
    }
```


'''


import asyncio
import aiohttp
import time
from aiolimiter import AsyncLimiter
import sqlite3
from ms_util_db import mark_bad_json, get_recs_bad_json

limiter = AsyncLimiter(1, 0.125)
USER_AGENT = ''
headers={'User-Agent':USER_AGENT}

INSERT_URL_ACCESS = r'INSERT INTO url_access (game_number, url, status, json, json_valid) VALUES (?, ?, ?, ?, ?);'
UPDATE_URL_ACCESS = r'UPDATE url_access set  url=?, status=?, json=?, json_valid=? WHERE game_number=?;'

DATABASE_PATH = 'async_records.db'
BASE_URL = '''https://servicebus2.caixa.gov.br/portaldeloterias/api/megasena/'''


async def web_scrape_task(game_id: int, semaphore) -> None:
    request_resp = await game_get(game_id, semaphore)
    await insert_request_resp(game_id, request_resp)


async def game_get(game_id: int, semaphore) -> bytes:
    url = f'{BASE_URL}{game_id}'
    async with aiohttp.ClientSession(headers=headers) as session:
        await semaphore.acquire()
        async with limiter:
            print(f"Begin downloading {url} {(time.perf_counter() - s):0.4f} seconds")
            async with session.get(url) as resp:
                content = await resp.read()
                print(f"Finished downloading {url}")
                semaphore.release()
                return tuple((url,content))

async def insert_request_resp(game_id: int, request_resp: bytes) -> None:
    '''
    Insere um registro na tabela "url_access"
    com o PK = game_id

    '''

    sql_conn = sqlite3.connect(DATABASE_PATH)
    cursor = sql_conn.cursor()

    try:

        cursor.execute(
            INSERT_URL_ACCESS, (game_id, request_resp[0], 0, request_resp[1].decode('utf-8'), 1)
        )  # campos: id, url, status=0, json, json_valid)
        sql_conn.commit()
        # DILEMA - marcar o "json_valid" como "True (=1)" para qualquer registro.
        # Adicionar aqui um validador no meio do "request" parece algo ruim
        # e que pode ser resolvido com 1 "update" no DB. Então por hora ficamos assim.
        # tudo é válido até que se prove o contrário ja que não temos uma msg de erro descente do serviço.


    except sqlite3.IntegrityError:

        # TODO: fazer um update se houver um json valido ?!?
        # todo: retirar o print e usar LOG
        print (f' [UPDATE] Atualizando registro  já existente - {game_id}')

        cursor.execute(
            # 'UPDATE url_access set url=?, status=?, json=?, json_valid=? where hame_number=
            UPDATE_URL_ACCESS, ( request_resp[0], 0, request_resp[1].decode('utf-8'), 1, game_id)
        )
        sql_conn.commit()

    finally:
        cursor.close()
        sql_conn.close()



async def main(game_list) -> None:
    tasks = []
    semaphore = asyncio.Semaphore(value=8)
    for game_id in game_list:
        tasks.append(web_scrape_task(game_id, semaphore))
    await asyncio.wait(tasks)



if __name__ == "__main__":


    game_list=[329, 367, 369, 370, 497, 507, 1045, 1048, 1631, 1766, 1770, 1777, 2098, 2101, 2469]

    s = time.perf_counter()
    asyncio.run(main(game_list))

    mark_bad_json(DATABASE_PATH)  # funcao do utils para marcar json sem info

    print(f'Registros marcados como tendo JSON Inválido - { len( list( get_recs_bad_json(DATABASE_PATH)))}')

    elapsed = time.perf_counter() - s
    print(f"Execution time: {elapsed:0.2f} seconds.")