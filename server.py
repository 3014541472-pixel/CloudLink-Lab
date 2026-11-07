"""CloudLink Lab: local, standard-library-only Bemfa TCP bridge.
Credentials remain in this process's memory. Virtual devices live in index.html.
"""
import collections
import json
import re
import socket
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent


class Broker:
    def __init__(self, uid, data, control):
        self.uid, self.topics = uid, {"telemetry": data, "control": control}
        self.sock = None
        self.connected = False
        self.ready = threading.Event()
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.events = collections.deque(maxlen=1000)
        self.queue_lock = threading.Lock()
        self.error = "订阅未确认，请检查私钥和 TCP 主题"

    def event(self, item):
        with self.queue_lock:
            self.events.append(item)

    def drain(self):
        with self.queue_lock:
            items = list(self.events)
            self.events.clear()
        return items

    def send(self, text):
        with self.lock:
            if not self.sock or self.stop.is_set():
                raise ConnectionError("not connected")
            self.sock.sendall((text + "\r\n").encode("utf-8"))

    def open(self):
        try:
            self.sock = socket.create_connection(("bemfa.com", 8344), timeout=4)
            self.sock.settimeout(1)
            threading.Thread(target=self.receive, daemon=True).start()
            self.send("cmd=1&uid=" + self.uid + "&topic=" + ",".join(self.topics.values()))
            if not self.ready.wait(5) or not self.connected:
                self.close()
                return False
            return True
        except (OSError, ValueError):
            self.error = "无法连接巴法云，请检查网络、账号和 TCP 主题"
            self.close()
            return False

    def receive(self):
        buffer = b""
        last_ping = time.monotonic()
        try:
            while not self.stop.is_set():
                if time.monotonic() - last_ping > 25:
                    self.send("ping")
                    last_ping = time.monotonic()
                try:
                    chunk = self.sock.recv(4096)
                except socket.timeout:
                    continue
                if not chunk:
                    raise ConnectionError("closed")
                buffer += chunk
                if len(buffer) > 65536:
                    raise ValueError("frame too large")
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    self.handle(line.decode("utf-8", errors="replace").strip())
        except (OSError, ValueError, AttributeError):
            if not self.stop.is_set():
                self.event({"type": "error", "message": "巴法云连接中断，请重新连接"})
        finally:
            self.connected = False
            self.ready.set()

    def handle(self, line):
        # Do not log the raw frame: Bemfa may echo the account private key.
        fields = dict(part.split("=", 1) for part in line.split("&") if "=" in part)
        if fields.get("cmd") == "1" and "res" in fields:
            self.connected = fields["res"] == "1"
            self.ready.set()
        if "msg" not in fields:
            return
        topic = fields.get("topic", "")
        channel = next((k for k, v in self.topics.items() if v == topic), None)
        if channel is None:
            return
        try:
            payload = json.loads(fields["msg"])
            if isinstance(payload, dict):
                self.event({"type": "message", "channel": channel, "payload": payload})
        except (json.JSONDecodeError, ValueError):
            pass

    def publish(self, channel, payload):
        if not self.connected:
            return False
        message = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
        if "&" in message or "\r" in message or "\n" in message or len(message) > 512:
            return False
        try:
            self.send("cmd=2&uid=" + self.uid + "&topic=" + self.topics[channel] + "&msg=" + message)
            return True
        except (OSError, KeyError):
            self.connected = False
            return False

    def close(self):
        self.stop.set()
        self.connected = False
        sock, self.sock = self.sock, None
        if sock:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        self.uid = ""


broker = None
broker_lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def reply(self, data, code=200):
        raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def allowed(self):
        # Bind only to loopback; reject foreign Host/Origin and cross-site requests.
        host = self.headers.get("Host", "")
        if host not in {"127.0.0.1:8765", "localhost:8765"}:
            return False
        origin = self.headers.get("Origin")
        if origin and origin not in {"http://127.0.0.1:8765", "http://localhost:8765"}:
            return False
        return self.headers.get("Sec-Fetch-Site") != "cross-site"

    def do_GET(self):
        if not self.allowed():
            self.reply({"error": "仅支持本机同源访问"}, 403)
            return
        path = urlsplit(self.path).path
        if path == "/api/health":
            self.reply({"app": "cloudlink"})
        elif path == "/api/events":
            b = broker
            self.reply({"connected": bool(b and b.connected), "events": b.drain() if b else []})
        elif path in ("/", "/index.html"):
            raw = (ROOT / "index.html").read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(raw)
        else:
            self.reply({"error": "not found"}, 404)

    def do_POST(self):
        global broker
        if not self.allowed() or self.headers.get("Content-Type", "").split(";")[0] != "application/json":
            self.reply({"error": "仅支持本机同源 JSON 请求"}, 403)
            return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 4096:
                raise ValueError()
            data = json.loads(self.rfile.read(size))
            if not isinstance(data, dict):
                raise ValueError()
        except (ValueError, json.JSONDecodeError):
            self.reply({"error": "invalid request"}, 400)
            return
        path = urlsplit(self.path).path
        if path == "/api/connect":
            uid, topic_data, topic_control = (str(data.get(k, "")) for k in ("uid", "data", "control"))
            if not re.fullmatch(r"[A-Za-z0-9]{8,128}", uid) or not all(re.fullmatch(r"[A-Za-z0-9_]{1,64}", t) for t in (topic_data, topic_control)) or topic_data == topic_control:
                self.reply({"ok": False, "error": "私钥或主题格式不正确"})
                return
            with broker_lock:
                if broker:
                    broker.close()
                broker = Broker(uid, topic_data, topic_control)
                ok = broker.open()
                self.reply({"ok": ok, "error": None if ok else broker.error})
        elif path == "/api/publish":
            channel, payload = data.get("channel"), data.get("payload")
            if channel not in ("telemetry", "control") or not isinstance(payload, dict):
                self.reply({"ok": False}, 400)
                return
            self.reply({"ok": bool(broker and broker.publish(channel, payload))})
        elif path == "/api/disconnect":
            with broker_lock:
                if broker:
                    broker.close()
                    broker = None
            self.reply({"ok": True})
        else:
            self.reply({"error": "not found"}, 404)


if __name__ == "__main__":
    try:
        httpd = ThreadingHTTPServer(("127.0.0.1", 8765), Handler)
    except OSError:
        print("端口 8765 已被占用。请关闭先前的 CloudLink 程序后重试。")
        raise SystemExit(1)
    print("CloudLink Lab 已启动：http://127.0.0.1:8765")
    print("浏览器未自动打开时，请复制上面的地址。按 Ctrl+C 退出。")
    threading.Timer(0.7, lambda: webbrowser.open("http://127.0.0.1:8765")).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if broker:
            broker.close()
        httpd.server_close()
