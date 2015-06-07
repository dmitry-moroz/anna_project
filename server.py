import time
import urllib
import json
import BaseHTTPServer
from urlparse import urlparse
from SocketServer import ThreadingMixIn
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

        code: return code to send
        msg: json data to send
        """
        self.send_response(code)
        self.send_header("Content-type",
                         "text/html; charset={0}".format(self.CODING))
        self.end_headers()
        if msg is not None:
            msg = json.dumps(msg)
            self.wfile.write(msg.encode(self.CODING))

    def random_fill(self, data):
        """Fills database with random data.

        data: json data with parameters for random filling
        """
        DataBase().insert_random_data(
            rows=data.get(u'rows', 1),
            goods=data.get(u'goods', ['default_name']),
            max_count=data.get(u'max_count', 10),
            min_date=data.get(u'min_date', -31536000))
        self.response(200)

    def insert(self, data):
        """Inserts specified record to database.

        data: json data with parameters of record for inserting
        """
        DataBase().insert_data(data[u'code'], data[u'date'], data[u'count'])
        self.response(200)

    def get_all(self):
        """Returns all records from database."""
        result = DataBase().get_data()
        self.response(200, result)

    def get_periods(self, records, s_date, f_date, f_type):
        """Divides records to periods according to type of period.

        records: all records to divide
        s_date: first date of periods
        f_date: last date of periods
        f_type: type of period to divide ('y'-year, 'm'-month, 'w'-week)
        """
        s_year = time.gmtime(s_date).tm_year
        s_mon = time.gmtime(s_date).tm_mon
        s_week = int(time.strftime("%U", time.gmtime(s_date)))

        f_year = time.gmtime(f_date).tm_year
        f_mon = time.gmtime(f_date).tm_mon
        f_week = int(time.strftime("%U", time.gmtime(f_date)))

        periods = []

        if f_type == u'y':
            for y in xrange(s_year, f_year + 1):
                period = 0
                for r in records:
                    if time.gmtime(r[1]).tm_year == y:
                        period += r[2]
                periods.append(float(period))

        elif f_type == u'm':
            for y in xrange(s_year, f_year + 1):
                x1 = s_mon if y == s_year else 1
                x2 = f_mon if y == f_year else 12
                for m in xrange(x1, x2 + 1):
                    period = 0
                    for r in records:
                        if time.gmtime(r[1]).tm_year == y and time.gmtime(r[1]).tm_mon == m:
                            period += r[2]
                    periods.append(float(period))

        elif f_type == u'w':
            for y in xrange(s_year, f_year + 1):
                yw = int(time.strftime("%U", time.strptime("31 Dec {0}".format(y), "%d %b %Y")))
                x1 = s_week if y == s_year else 0
                x2 = f_week if y == f_year else yw
                for w in xrange(x1, x2 + 1):
                    period = 0
                    for r in records:
                        if time.gmtime(r[1]).tm_year == y and int(time.strftime("%U", time.gmtime(r[1]))) == w:
                            period += r[2]
                    periods.append(float(period))

        return periods

    def forecast(self, data):
        """Provides forecast for next period.

        data: json data with parameters for forecasting
        """
        goods_code = data.get(u'code', None)
        f_type = data.get(u'f_type', 'm')
        s_date = data.get(u's_date', 0)
        f_date = data.get(u'f_date', time.time())

        if goods_code is None:
            json_data = {u'error': u'Good\'s code is not specified'}
            self.response(418, json_data)
            return False

        records = DataBase().get_data(code=goods_code, date=s_date)
        records = sorted(records, key=lambda r: r[1])
        periods = self.get_periods(records, s_date, f_date, f_type)

        alphas = map(lambda x: float(x)/100, range(5, 31, 5))
        best_mad = None
        best_forecasts = None
        for a in alphas:
            forecasts = []
            for i in xrange(len(periods)):
                if i == 0:
                    continue
                elif i == 1:
                    forecasts.append(periods[i - 1])
                else:
                    f = (1 - a)*periods[i - 1] + a*forecasts[-1]
                    forecasts.append(f)
            mad = 0
            for p, f in zip(periods[1:], forecasts):
                mad += abs(p - f)
            mad = mad/len(forecasts)
            if best_mad is None or mad < best_mad:
                best_mad = mad
                best_forecasts = forecasts

        j_data = {'code': goods_code, 'f_type': f_type,
                  's_date': s_date, 'f_date': f_date,
                  'periods': periods, 'forecasts': best_forecasts,
                  'mad': best_mad, 'forecast': best_forecasts[-1]}
        self.response(200, j_data)

    def do_GET(self):
        """Respond to a GET request."""
        request = urllib.unquote(self.path).decode('UTF-8')
        try:
            request = urlparse(request)
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
    """Handle requests in a separate thread."""

# Main program starting
if __name__ == '__main__':
    from db_worker import DataBase

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
