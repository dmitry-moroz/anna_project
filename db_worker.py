import string
import MySQLdb
import random
import time
from cache_meta import CacheMeta
from config import db_conf, cache_conf


class DataBase(object):

    if cache_conf['enable_cache']:
        __metaclass__ = CacheMeta

    db_name = db_conf['name']
    table = db_conf['table']
    host = db_conf['host']
    user = db_conf['user']
    pswd = db_conf['password']

    def __init__(self):
        self._connection = None
        self._cursor = None

    def __del__(self):
        if self._connection:
            self._connection.close()

    @property
    def connection(self):
        if not self._connection:
            self._connection = MySQLdb.connect(
                host=self.host,
                user=self.user,
                passwd=self.pswd,
                db=self.db_name
            )
        return self._connection

    @property
    def cursor(self):
        if not self._cursor:
            self._cursor = self.connection.cursor()
        return self._cursor

    def insert_data(self, n_code, n_date, n_count):
        self.cursor.execute(
            'INSERT INTO {3} '
            '(name, amount, date) '
            'VALUES("{0}", {1}, {2})'
            .format(n_code, n_count, n_date, self.table)
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
                '(name, amount, date) '
                'VALUES("{0}", {1}, {2})'
                .format(r_code, r_count, r_date, self.table)
            )
        self.connection.commit()

    def get_data(self, code=None, date=None):
        self.cursor.execute('SELECT name, amount, date FROM {0}'
                            .format(self.table))
        result = self.cursor.fetchall()
        if date:
            result = filter(lambda r: r[1] > date, result)
        if code:
            result = filter(lambda r: r[0] == code, result)
        return result

    def prepare_table(self):
        self.cursor.execute("DROP TABLE IF EXISTS {0}".format(self.table))
        self.connection.commit()
        self.cursor.execute('CREATE TABLE {0} '
                            '(store_id INT NOT NULL AUTO_INCREMENT, '
                            'name CHAR(64) NOT NULL, '
                            'amount INT NOT NULL, '
                            'date INT NOT NULL,'
                            'PRIMARY KEY (store_id))'.format(self.table))
        self.connection.commit()
