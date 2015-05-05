import string
import sqlite3
import random
import json
import time
from cache_meta import CacheMeta
from config import db_conf


class DataBase(object):

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
        pass

    def insert_random_data(self, rows=1):
        for i in xrange(rows):
            r_code = ''.join(random.choice(string.hexdigits)
                             for i in xrange(20))
            r_date = time.time() + random.randint(-31536000, 31536000)
            r_count = random.randint(1, 10)
            self.cursor.execute(
                'INSERT INTO {3} '
                '(code, date, count) '
                'VALUES("{0}", {1}, {2})'
                .format(r_code, r_date, r_count, self.table)
            )
        self.connection.commit()

    def get_data(self):
        self.cursor.execute('SELECT * FROM {0}'.format(self.table))
        return self.cursor.fetchall()

    def forecasting(self):
        pass

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
