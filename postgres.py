import psycopg2
from psycopg2 import extras
from contextlib import closing
from typing import List, Tuple

from airflow.hooks.base import BaseHook

BATCH_SIZE = 1000


def get_pg_params(conn_id: str) -> dict:
    if 'postgresql://' not in conn_id:
        hook = BaseHook.get_connection(conn_id)
        return {
            "host": hook.host,
            "port": hook.port,
            "user": hook.login,
            "password": hook.password,
            "dbname": hook.schema
        }
    return {"dsn": conn_id}


def execute_query(pg_conn_id: str, query: str, values: Tuple = None) -> None:
    pg_params = get_pg_params(conn_id=pg_conn_id)
    with closing(psycopg2.connect(**pg_params)) as pg_conn, closing(pg_conn.cursor()) as pg_cursor:
        pg_cursor.execute(query, values)
        pg_conn.commit()


def execute_query_and_fetchall(pg_conn_id: str, query: str, values: Tuple = None) -> List[Tuple]:
    pg_params = get_pg_params(conn_id=pg_conn_id)
    with closing(psycopg2.connect(**pg_params)) as pg_conn, closing(pg_conn.cursor()) as pg_cursor:
        pg_cursor.execute(query, values)
        record = pg_cursor.fetchall()
        pg_conn.commit()
    return record


def execute_batch(pg_conn_id: str, query: str, records: List[Tuple], batch_size: int = BATCH_SIZE) -> None:
    pg_params = get_pg_params(conn_id=pg_conn_id)
    with closing(psycopg2.connect(**pg_params)) as pg_conn, closing(pg_conn.cursor()) as pg_cursor:
        extras.execute_batch(
            cur=pg_cursor,
            sql=query,
            argslist=records,
            page_size=batch_size
        )
        pg_conn.commit()


def fetchall(pg_conn_id: str, query: str) -> List[Tuple]:
    pg_params = get_pg_params(conn_id=pg_conn_id)
    with closing(psycopg2.connect(**pg_params)) as pg_conn, closing(pg_conn.cursor()) as pg_cursor:
        pg_cursor.execute(query)
        records = pg_cursor.fetchall()
        return records


def fetchall_with_columns(pg_conn_id: str, query: str) -> Tuple[List[Tuple], List[str]]:
    pg_params = get_pg_params(conn_id=pg_conn_id)
    with closing(psycopg2.connect(**pg_params)) as pg_conn, closing(pg_conn.cursor()) as pg_cursor:
        pg_cursor.execute(query)
        columns = [desc[0] for desc in pg_cursor.description]
        records = pg_cursor.fetchall()
        return records, columns


def fetchone(pg_conn_id: str, query: str) -> List[Tuple]:
    pg_params = get_pg_params(conn_id=pg_conn_id)
    with closing(psycopg2.connect(**pg_params)) as pg_conn, closing(pg_conn.cursor()) as pg_cursor:
        pg_cursor.execute(query)
        record = pg_cursor.fetchone()[0]
        return record
