import time

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from transactions import TRANSACTION_1
from config import local_env

engine = create_engine('postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:5432/{POSTGRES_DB}'.format(**local_env))
session = scoped_session(sessionmaker(bind=engine))


@contextmanager
def timed(fmt, **kwargs):
    start = time.time()
    yield
    print(fmt.format(**{**kwargs, 'time': time.time() - start}))


def execute_statement(sql, **kwargs):
    """:returns list of dicts, where each dict is a row"""
    sql = sql.format(**kwargs)
    with engine.connect() as con:
        con.execute(sql)
        # results = [
        #     {
        #         column: value
        #         for (column, value) in row_proxy.items()
        #     }
        #     for row_proxy in con.execute(sql)
        # ]
    return []


if __name__ == '__main__':
    sql = 'SELECT * FROM pg_database;'
    # print('Example rows:')
    # for row in execute_statement(TRANSACTION_1):
    #     print(row)

    print('Example timed:')
    with timed('{sql} statement took {time:.3f}s', sql=TRANSACTION_1):
        execute_statement(TRANSACTION_1)
