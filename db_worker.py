import string
import sqlite3
import random
import time
from cache_meta import CacheMeta
from config import db_conf, cache_conf


class DataBase(object):

    if cache_conf['enable_cache']:
        __metaclass__ = CacheMeta

    db_name = db_conf['name']
    table = db_conf['table']

    def __init__(self):
        self._connection = None
        self._cursor = None

    def __del__(self):
        if self._connection:
            self._connection.close()

    @property
    def connection(self):
        if not self._connection:
            self._connection = sqlite3.connect(self.db_name)
        return self._connection

    @property
    def cursor(self):
        if not self._cursor:
            self._cursor = self.connection.cursor()
        return self._cursor

    def insert_data(self, n_code, n_date, n_count):
        self.cursor.execute(
            'INSERT INTO {3} '
            '(code, date, count) '
            'VALUES("{0}", {1}, {2})'
            .format(n_code, n_date, n_count, self.table)
        )
        self.connection.commit()

    def insert_random_data(self, rows=200, goods=50, max_count=5,
                           min_date=-31536000, code_len=20):
        codes = []
        for g in xrange(goods):
            codes.append(''.join(random.choice(string.hexdigits)
                                 for i in xrange(code_len)))
        for i in xrange(rows):
            r_code = random.choice(codes)
            r_date = time.time() + random.randint(min_date, 0)
            r_count = random.randint(1, max_count)
            self.cursor.execute(
                'INSERT INTO {3} '
                '(code, date, count) '
                'VALUES("{0}", {1}, {2})'
                .format(r_code, r_date, r_count, self.table)
            )
        self.connection.commit()

    def get_data(self, code=None, date=None):
        self.cursor.execute('SELECT * FROM {0}'.format(self.table))
        result = self.cursor.fetchall()
        if date:
            result = filter(lambda r: r[1] > date, result)
        if code:
            result = filter(lambda r: r[0] == code, result)
        return result

    def prepare_table(self):
        self.cursor.execute('SELECT tbl_name FROM sqlite_master '
                            'WHERE type="table"')
        if self.table in str(self.cursor.fetchall()):
            self.cursor.execute('DROP TABLE {0}'.format(self.table))
            self.connection.commit()
        self.cursor.execute('CREATE TABLE {0} '
                            '(code VARCHAR(32), '
                            'date INTEGER, '
                            'count INTEGER)'.format(self.table))
        self.connection.commit()
