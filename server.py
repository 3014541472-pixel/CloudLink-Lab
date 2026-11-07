"""CloudLink Lab local application. Python 3.10+, standard library only."""
import argparse
import csv
import io
import json
import math
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from engine import Lab

ROOT = Path(__file__).resolve().parent
STATIC_FILES = {
    '/': ('index.html', 'text/html; charset=utf-8'),
    '/index.html': ('index.html', 'text/html; charset=utf-8'),
    '/app.js': ('app.js', 'text/javascript; charset=utf-8'),
    '/style.css': ('style.css', 'text/css; charset=utf-8'),
    '/preview.html': ('preview.html', 'text/html; charset=utf-8'),
}


def query_options(query):
    device = query.get('device', ['ENV-001'])[0]
    try:
        limit = int(query.get('limit', ['120'])[0])
        after = float(query.get('after', ['0'])[0])
    except (ValueError, OverflowError):
        raise ValueError('历史筛选条件无效') from None
    if not 1 <= limit <= 500 or not math.isfinite(after) or after < 0:
        raise ValueError('历史筛选条件超出范围')
    return device, limit, after


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    @property
    def lab(self):
        return self.server.lab

    def send_bytes(self, raw, content_type, code=200, filename=None):
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(raw)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        if filename:
            self.send_header('Content-Disposition', 'attachment; filename="' + filename + '"')
        self.end_headers()
        try:
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def reply(self, data, code=200):
        raw = json.dumps(data, ensure_ascii=False, allow_nan=False).encode('utf-8')
        self.send_bytes(raw, 'application/json; charset=utf-8', code)

    def allowed(self):
        port = self.server.server_port
        allowed_hosts = {f'127.0.0.1:{port}', f'localhost:{port}'}
        allowed_origins = {'http://' + host for host in allowed_hosts}
        origin = self.headers.get('Origin')
        return (self.headers.get('Host') in allowed_hosts
                and (not origin or origin in allowed_origins)
                and self.headers.get('Sec-Fetch-Site') != 'cross-site')

    def do_GET(self):
        if not self.allowed():
            self.reply({'error': '仅支持本机同源访问'}, 403)
            return
        request = urlsplit(self.path)
        try:
            if request.path == '/api/health':
                self.reply({'app': 'cloudlink', 'version': '2.0.0'})
            elif request.path == '/api/state':
                device, limit, after = query_options(parse_qs(request.query))
                self.reply(self.lab.snapshot(device, limit, after))
            elif request.path == '/api/export':
                self.export_csv(parse_qs(request.query))
            elif request.path in STATIC_FILES:
                filename, content_type = STATIC_FILES[request.path]
                self.send_bytes((ROOT / filename).read_bytes(), content_type)
            else:
                self.reply({'error': '页面不存在'}, 404)
        except ValueError as error:
            self.reply({'error': str(error)}, 400)

    def export_csv(self, query):
        device, _, after = query_options(query)
        if device not in self.lab.devices:
            raise ValueError('设备不存在')
        rows = self.lab.db.rows(
            'SELECT device_id,received_at,temperature,humidity,fan,mode FROM telemetry '
            'WHERE device_id=? AND received_at>=? ORDER BY sequence DESC LIMIT 100000', (device, after),
        )
        output = io.StringIO(newline='')
        output.write('\ufeff')
        writer = csv.writer(output)
        writer.writerow(['device_id', 'received_at_unix', 'temperature_C', 'humidity_RH', 'fan', 'mode'])
        for row in reversed(rows):
            writer.writerow(row.values())
        self.send_bytes(output.getvalue().encode('utf-8'), 'text/csv; charset=utf-8', filename=device + '.csv')

    def do_POST(self):
        if not self.allowed() or self.headers.get('Content-Type', '').split(';')[0] != 'application/json':
            self.reply({'error': '仅支持本机同源 JSON 请求'}, 403)
            return
        try:
            size = int(self.headers.get('Content-Length', '0'))
            if not 0 < size <= 4096:
                raise ValueError('请求过大或为空')
            data = json.loads(self.rfile.read(size))
            if not isinstance(data, dict):
                raise ValueError('请求须为 JSON 对象')
            self.mutate(urlsplit(self.path).path, data)
        except (ValueError, TypeError, OverflowError):
            self.reply({'error': '参数无效，请检查设备、模式、主题和私钥格式'}, 400)

    def mutate(self, path, data):
        if path == '/api/connect':
            # Accept the original topic field names so saved connection instructions remain valid.
            self.lab.configure_cloud(data.get('uid'), data.get('data'), data.get('control'))
            self.reply({'ok': True, 'state': 'connecting'})
        elif path == '/api/disconnect':
            self.lab.change_transport()
            self.reply({'ok': True})
        elif path == '/api/device':
            changes = data.get('changes')
            if not isinstance(changes, dict) or not isinstance(data.get('device_id'), str):
                raise ValueError('设备设置无效')
            self.lab.configure_device(data['device_id'], changes)
            self.reply({'ok': True})
        elif path == '/api/command':
            if not isinstance(data.get('device_id'), str) or not isinstance(data.get('action'), str):
                raise ValueError('指令无效')
            command = self.lab.issue_command(data['device_id'], data['action'], data.get('id'))
            self.reply({'ok': True, 'command': command})
        else:
            self.reply({'error': '接口不存在'}, 404)


def create_server(port=8765, database_path=None, start_devices=True):
    lab = Lab(database_path or ROOT / 'data' / 'cloudlink.sqlite3')
    try:
        server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    except OSError:
        lab.close()
        raise
    server.daemon_threads = True
    server.lab = lab
    if start_devices:
        lab.start()
    return server


def main():
    parser = argparse.ArgumentParser(description='CloudLink Lab local application')
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--no-browser', action='store_true')
    parser.add_argument('--database', type=Path, help='SQLite file path')
    args = parser.parse_args()
    try:
        server = create_server(args.port, args.database)
    except OSError:
        print('端口已被占用，请关闭旧版 CloudLink 窗口后重试。')
        return 1
    url = f'http://127.0.0.1:{server.server_port}'
    print('CloudLink Lab 2.0 已启动 ' + url)
    print('三个虚拟设备正在运行，关闭网页后仍可采样。按 Ctrl+C 退出程序。')
    if not args.no_browser:
        threading.Timer(0.7, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
        server.lab.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
