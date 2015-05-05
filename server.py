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
    SHELL = u'/shell'
    GET_ALL = u'/get_all'
    RANDOM_FILL = u'/random_fill'

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

    def run_cmd(self, cmd):
        """Performs command into subprocess routine and returns stdout,
        stderr and return code.
        cmd: target command for performing
        """
        cmd = cmd.encode(sys.stdin.encoding)
        process_cmd = subprocess.Popen(cmd, shell=True,
                                       stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)

        output, stderr = process_cmd.communicate()
        return (output.decode(sys.stdout.encoding),
                stderr.decode(sys.stdout.encoding),
                process_cmd.returncode)

    def shell(self, cmd):
        out, err, code = self.run_cmd(cmd)
        if not code:
            self.response(200, out)
        else:
            self.response(418, out)

    def random_fill(self, data):
        DataBase().insert_random_data(int(data))
        self.response(200)

    def get_all(self):
        result = DataBase().get_data()
        self.response(200, result)

    def do_GET(self):
        """Respond to a GET request.
        """
        request = urllib.unquote(self.path).decode('UTF-8')
        try:
            request = urlparse(request)
            data = request.params
            #data = json.loads(request.params) if request.params else ''

            if request.path == self.SHELL:
                self.shell(data)

            elif request.path == self.PING:
                json_data = {'info': 'Forecasting server is running'}
                self.response(200, json_data)

            elif request.path == self.GET_ALL:
                self.get_all()

            elif request.path == self.RANDOM_FILL:
                self.random_fill(data)

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
