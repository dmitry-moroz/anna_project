import time
import subprocess
import sys
import string
import urllib
import json
import BaseHTTPServer
from urlparse import urlparse
from SocketServer import ThreadingMixIn
from db_worker import DataBase
from config import srv_conf, db_conf


class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    CODING = 'UTF-8'
    PING = u'/'
    GET_ALL = u'/get_all'
    RANDOM_FILL = u'/random_fill'
    INSERT = u'/insert'
    FORECAST = u'/forecast'

    def response(self, code, msg=None):
        """Sends specified return code and provides header.
        code: return code for sending
        """
        self.send_response(code)
        self.send_header("Content-type",
                         "text/html; charset={0}".format(self.CODING))
        self.end_headers()
        if msg is not None:
            msg = json.dumps(msg)
            self.wfile.write(msg.encode(self.CODING))

    def random_fill(self, data):
        DataBase().insert_random_data(rows=data.get(u'rows', 1),
                                      goods=data.get(u'goods', 50),
                                      max_count=data.get(u'max_count', 5),
                                      min_date=data.get(u'min_date', -31536000),
                                      code_len=data.get(u'code_len', 20))
        self.response(200)

    def insert(self, data):
        DataBase().insert_data(data[u'code'], data[u'date'], data[u'count'])
        self.response(200)

    def get_all(self):
        result = DataBase().get_data()
        self.response(200, result)

    def forecast(self, data):
        """
        data:
        f_type: y, m, w
        """
        goods_code = data.get(u'code', None)
        f_type = data.get(u'f_type', 'm')
        s_date = data.get(u's_date', None)
        f_date = data.get(u'f_date', time.time())

        if goods_code is None:
            json_data = {u'error': u'Good\'s code is not specified'}
            self.response(418, json_data)
            return False

        records = DataBase().get_data(code=goods_code, date=s_date)
        #NO sales error
        records = sorted(records, key=lambda r: r[1])

        f_year = time.gmtime(f_date).tm_year
        f_mon = time.gmtime(f_date).tm_mon
        f_day = time.gmtime(f_date).tm_yday
        periods = {}

        if f_type == u'y':
            for y in xrange(time.gmtime(records[0][1]).tm_year, f_year + 1):
                period = 0
                for r in records:
                    if time.gmtime(r[1]).tm_year == y:
                        period += r[2]
                periods.update({str(y): period})

        elif f_type == u'm':
            for y in xrange(time.gmtime(records[0][1]).tm_year, f_year + 1):
                x = f_mon if y == f_year else 12
                for m in xrange(1, x + 1):
                    period = 0
                    for r in records:
                        if time.gmtime(r[1]).tm_year == y and time.gmtime(r[1]).tm_mon == m:
                            period += r[2]
                    periods.update({'.'.join([str(y), str(m)]): period})

        elif f_type == u'w':
            for y in xrange(time.gmtime(records[0][1]).tm_year, f_year + 1):
                weeks = []
                for i in range(1, 367)[::7]:
                    weeks.append(range(1, 367)[i - 1:i + 6])
                for w in xrange(0, len(weeks)):
                    if y == f_year and f_day in weeks[w - 1]:
                        break
                    period = 0
                    for r in records:
                        if time.gmtime(r[1]).tm_year == y and time.gmtime(r[1]).tm_yday in weeks[w]:
                            period += r[2]
                    periods.update({'.'.join([str(y), str(w)]): period})



        alphas = map(lambda x: float(x)/100, range(5, 31, 5))
        best_mad = None
        best_forecasts = None
        for a in alphas:
            forecasts = []
            for i in xrange(len(records) + 1):
                if i == 0:
                    continue
                elif i == 1:
                    pass




    def do_GET(self):
        """Respond to a GET request.
        """
        request = urllib.unquote(self.path).decode('UTF-8')
        try:
            request = urlparse(request)
            #data = request.params
            data = json.loads(request.params) if request.params else {}

            if request.path == self.PING:
                json_data = {u'info': u'Forecasting server is running'}
                self.response(200, json_data)

            elif request.path == self.GET_ALL:
                self.get_all()

            elif request.path == self.RANDOM_FILL:
                self.random_fill(data)

            elif request.path == self.INSERT:
                self.insert(data)

            elif request.path == self.FORECAST:
                self.forecast(data)

            else:
                self.response(404)
        except:
            self.response(400)


class ThreadedHTTPServer(ThreadingMixIn, BaseHTTPServer.HTTPServer):
    """Handle requests in a separate thread.
    """


if __name__ == '__main__':
    httpd = ThreadedHTTPServer((srv_conf['host_name'], srv_conf['port']),
                               MyHandler)
    print "[{2}] Server Starts - {0}:{1}".format(
        srv_conf['host_name'], srv_conf['port'], time.asctime())
    try:
        if db_conf['drop_existing_db']:
            DataBase().prepare_table()
        httpd.serve_forever()
    except:
        pass
    httpd.server_close()
    print "[{2}] Server Stops - {0}:{1}".format(
        srv_conf['host_name'], srv_conf['port'], time.asctime())
