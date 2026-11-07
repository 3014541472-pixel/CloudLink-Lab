"""Bemfa TCP transport with confirmed subscription, heartbeat, and reconnection."""
import json
import socket
import threading
import time


class BemfaTransport:
    def __init__(self, uid, data_topic, control_topic, on_message, on_event,
                 address=('bemfa.com', 8344), retry_base=1.0):
        self.uid = uid
        self.topics = {'telemetry': data_topic, 'control': control_topic}
        self.on_message = on_message
        self.on_event = on_event
        self.address = address
        self.retry_base = retry_base
        self.stop_event = threading.Event()
        self.socket_lock = threading.Lock()
        self.sock = None
        self.connected = False
        self.state = 'connecting'
        self.reconnections = 0
        self.next_retry_at = None
        self.connected_since = None
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self.run, daemon=True, name='bemfa-transport')
        self.thread.start()

    def snapshot(self):
        return {'connected': self.connected, 'state': self.state,
                'reconnections': self.reconnections, 'next_retry_at': self.next_retry_at,
                'topics': self.topics.copy()}

    def send_frame(self, frame):
        with self.socket_lock:
            if self.sock is None or self.stop_event.is_set():
                raise ConnectionError('transport disconnected')
            self.sock.sendall((frame + '\r\n').encode('utf-8'))

    def publish(self, channel, payload):
        if not self.connected or channel not in self.topics:
            return False
        message = json.dumps(payload, separators=(',', ':'), ensure_ascii=True, allow_nan=False)
        if len(message) > 512 or '&' in message:
            return False
        try:
            self.send_frame('cmd=2&uid=' + self.uid + '&topic=' + self.topics[channel] + '&msg=' + message)
            return True
        except OSError:
            self.disconnect_socket()
            return False

    def disconnect_socket(self):
        self.connected = False
        with self.socket_lock:
            sock, self.sock = self.sock, None
        if sock:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()

    def run(self):
        failures = 0
        while not self.stop_event.is_set():
            self.state = 'connecting'
            self.next_retry_at = None
            self.connected_since = None
            try:
                sock = socket.create_connection(self.address, timeout=4)
                sock.settimeout(0.5)
                with self.socket_lock:
                    if self.stop_event.is_set():
                        sock.close()
                        break
                    self.sock = sock
                self.send_frame('cmd=1&uid=' + self.uid + '&topic=' + ','.join(self.topics.values()))
                self.receive(sock)
            except (OSError, ValueError):
                pass
            finally:
                self.disconnect_socket()
            if self.stop_event.is_set():
                break
            if self.connected_since and time.monotonic() - self.connected_since >= 30:
                failures = 0
            failures += 1
            delay = min(30, self.retry_base * (2 ** min(failures - 1, 5)))
            self.state = 'reconnecting'
            self.next_retry_at = time.time() + delay
            self.reconnections += 1
            self.on_event('云连接中断或订阅失败，将自动重连', 'error')
            if self.stop_event.wait(delay):
                break
        self.state = 'stopped'

    def receive(self, sock):
        buffer = b''
        started = last_receive = last_ping = time.monotonic()
        while not self.stop_event.is_set():
            now = time.monotonic()
            if not self.connected and now - started > 5:
                raise TimeoutError('subscription acknowledgement timed out')
            if now - last_receive > 65:
                raise TimeoutError('heartbeat timed out')
            if now - last_ping > 25:
                self.send_frame('ping')
                last_ping = now
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                continue
            if not chunk:
                raise ConnectionError('peer closed connection')
            last_receive = time.monotonic()
            buffer += chunk
            if len(buffer) > 65536:
                raise ValueError('oversized frame')
            while b'\n' in buffer:
                line, buffer = buffer.split(b'\n', 1)
                self.handle(line.decode('utf-8', errors='replace').strip())

    def handle(self, line):
        # Raw responses may contain uid. Only emit fixed, sanitized event messages.
        fields = dict(part.split('=', 1) for part in line.split('&') if '=' in part)
        if fields.get('cmd') == '1' and 'res' in fields:
            if fields['res'] != '1':
                raise ValueError('subscription rejected')
            self.connected = True
            self.connected_since = time.monotonic()
            self.state = 'connected'
            self.next_retry_at = None
            self.on_event('巴法云订阅已确认，数据通道就绪', 'info')
        if not self.connected or 'msg' not in fields:
            return
        channel = next((key for key, topic in self.topics.items() if topic == fields.get('topic')), None)
        if channel is None:
            return
        try:
            payload = json.loads(fields['msg'])
        except (ValueError, RecursionError):
            return
        if isinstance(payload, dict):
            self.on_message(channel, payload)

    def close(self):
        self.stop_event.set()
        self.disconnect_socket()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=5)
        self.uid = ''
