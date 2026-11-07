"""Regression checks for device isolation, persistence, and transport recovery."""
import json
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from engine import Lab
from server import create_server
from transport import BemfaTransport


class LabTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / 'lab.sqlite3'
        self.now = time.time()
        self.lab = Lab(self.path, clock=lambda: self.now)

    def tearDown(self):
        self.lab.close()
        self.directory.cleanup()

    def advance(self, seconds):
        for _ in range(round(seconds * 10)):
            self.now += 0.1
            self.lab.step()

    def test_device_isolation(self):
        self.lab.issue_command('ENV-001', 'fan_on', 'routing')
        self.advance(1)
        self.assertEqual(self.lab.db.command('routing')['status'], 'acknowledged')
        self.assertTrue(self.lab.models['ENV-001']['fan'])
        self.assertFalse(self.lab.models['ENV-002']['fan'])
        self.assertFalse(self.lab.models['ENV-003']['fan'])

    def test_lost_ack_retries_without_duplicate_execution(self):
        publish = self.lab.transmit
        dropped = []

        def drop_first_ack(channel, payload):
            if payload['kind'] == 'ack' and not dropped:
                dropped.append(payload)
                return True
            return publish(channel, payload)

        self.lab.transmit = drop_first_ack
        self.lab.issue_command('ENV-001', 'fan_on', 'retry')
        self.advance(2)
        command = self.lab.db.command('retry')
        self.assertEqual(command['attempts'], 2)
        self.assertEqual(command['status'], 'acknowledged')
        self.assertEqual(len(self.lab.db.rows("SELECT * FROM events WHERE message LIKE '设备执行%'")), 1)
        same = self.lab.issue_command('ENV-001', 'fan_on', 'retry')
        self.assertEqual(same['id'], 'retry')
        with self.assertRaises(ValueError):
            self.lab.issue_command('ENV-002', 'fan_on', 'retry')

    def test_timeout_does_not_execute_after_recovery(self):
        self.lab.configure_device('ENV-001', {'scenario': 'offline'})
        self.lab.issue_command('ENV-001', 'fan_on', 'offline')
        self.advance(6)
        command = self.lab.db.command('offline')
        self.assertEqual(command['attempts'], 3)
        self.assertEqual(command['status'], 'timeout')
        self.lab.configure_device('ENV-001', {'scenario': 'normal'})
        self.lab.receive('control', {'kind': 'command', 'id': 'offline', 'action': 'fan_on', 'device_id': 'ENV-001'})
        self.assertFalse(self.lab.models['ENV-001']['fan'])

    def test_ack_must_match_executed_device_and_value(self):
        self.lab.issue_command('ENV-001', 'fan_on', 'ack')
        self.lab.receive('telemetry', {'kind': 'ack', 'id': 'ack', 'fan': True, 'device_id': 'ENV-002'})
        self.assertEqual(self.lab.db.command('ack')['status'], 'pending')
        self.lab.execute_command('ENV-001', {'id': 'ack', 'action': 'fan_on'})
        self.lab.receive('telemetry', {'kind': 'ack', 'id': 'ack', 'fan': False, 'device_id': 'ENV-001'})
        self.assertEqual(self.lab.db.command('ack')['status'], 'pending')

    def test_history_configuration_receipts_survive_restart(self):
        self.lab.configure_device('ENV-002', {'threshold': 26, 'auto': True})
        self.lab.issue_command('ENV-001', 'fan_on', 'persist')
        self.advance(1)
        count = self.lab.snapshot()['stats']['samples']
        self.assertGreater(count, 0)
        self.lab.close()
        self.lab = Lab(self.path)
        self.assertEqual(self.lab.devices['ENV-002']['threshold'], 26)
        self.assertTrue(self.lab.devices['ENV-002']['auto'])
        self.assertTrue(self.lab.devices['ENV-001']['fan'])
        self.assertEqual(self.lab.snapshot()['stats']['samples'], count)
        self.assertEqual(self.lab.db.command('persist')['status'], 'acknowledged')

    def test_pending_commands_interrupted_on_restart(self):
        self.lab.issue_command('ENV-001', 'fan_on', 'interrupted')
        self.lab.close()
        self.lab = Lab(self.path)
        self.assertEqual(self.lab.db.command('interrupted')['status'], 'interrupted')
        self.assertFalse(self.lab.models['ENV-001']['fan'])

    def test_hysteresis_and_automatic_command(self):
        self.lab.configure_device('ENV-001', {'auto': True, 'threshold': 26})
        self.lab.auto_control('ENV-001', self.lab.devices['ENV-001'], {'temp': 27, 'fan': False}, self.now)
        self.advance(1)
        self.assertTrue(self.lab.models['ENV-001']['fan'])
        self.now += 9
        self.lab.auto_control('ENV-001', self.lab.devices['ENV-001'], {'temp': 25.5, 'fan': True}, self.now)
        self.assertEqual(len(self.lab.snapshot()['commands']), 1)
        self.lab.auto_control('ENV-001', self.lab.devices['ENV-001'], {'temp': 24.9, 'fan': True}, self.now)
        self.assertEqual(self.lab.snapshot()['commands'][0]['action'], 'fan_off')

    def test_samples_are_validated_and_deduplicated(self):
        sample = {'kind': 'telemetry', 'device_id': 'ENV-001', 'sample_id': 'one', 'temp': 27.5, 'humidity': 55, 'fan': False}
        self.lab.receive('telemetry', sample)
        self.lab.receive('telemetry', sample)
        self.lab.receive('telemetry', {**sample, 'sample_id': 'bad', 'temp': float('nan')})
        self.lab.receive('telemetry', {**sample, 'device_id': []})
        self.assertEqual(self.lab.snapshot()['stats']['samples'], 1)


class TransportTests(unittest.TestCase):
    def test_fragmented_frames_reconnect_and_resubscribe(self):
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen(2)
        listener.settimeout(6)
        received, subscriptions, failures = [], [], []
        restored, release = threading.Event(), threading.Event()

        def broker():
            try:
                for attempt in range(2):
                    peer, _ = listener.accept()
                    with peer:
                        peer.settimeout(3)
                        data = b''
                        while b'\r\n' not in data:
                            data += peer.recv(4096)
                        subscriptions.append(data)
                        peer.sendall(b'cmd=1&re')
                        peer.sendall(b's=1\r\ncmd=2&topic=cltelemetry004&msg={"kind":"telemetry"}\r\n')
                        if attempt == 0:
                            time.sleep(0.05)
                        else:
                            restored.set()
                            release.wait(3)
            except Exception as error:
                failures.append(error)

        thread = threading.Thread(target=broker, daemon=True)
        thread.start()
        transport = BemfaTransport('testkey123', 'cltelemetry004', 'clcontrol003',
                                   lambda channel, payload: received.append((channel, payload)),
                                   lambda *_: None, address=listener.getsockname(), retry_base=0.02)
        try:
            transport.start()
            self.assertTrue(restored.wait(5), 'transport did not reconnect')
            deadline = time.monotonic() + 2
            while len(received) < 2 and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(transport.connected)
            self.assertEqual(len(subscriptions), 2)
            self.assertEqual(len(received), 2)
            self.assertGreaterEqual(transport.reconnections, 1)
            self.assertIn(b'topic=cltelemetry004,clcontrol003\r\n', subscriptions[1])
            self.assertFalse(failures)
        finally:
            release.set()
            transport.close()
            listener.close()
            thread.join(timeout=3)


class CloudRoundtripTests(unittest.TestCase):
    def test_cloud_routes_three_devices_and_confirms_command(self):
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        listener.settimeout(5)
        stop = threading.Event()
        failures = []

        def broker():
            try:
                peer, _ = listener.accept()
                with peer:
                    peer.settimeout(0.2)
                    buffer = b''
                    while not stop.is_set():
                        try:
                            chunk = peer.recv(4096)
                        except socket.timeout:
                            continue
                        if not chunk:
                            break
                        buffer += chunk
                        while b'\n' in buffer:
                            line, buffer = buffer.split(b'\n', 1)
                            fields = dict(item.split('=', 1) for item in line.decode().strip().split('&') if '=' in item)
                            if fields.get('cmd') == '1':
                                peer.sendall(b'cmd=1&res=1\r\n')
                            elif fields.get('cmd') == '2':
                                frame = 'cmd=2&topic=' + fields['topic'] + '&msg=' + fields['msg'] + '\r\n'
                                peer.sendall(frame.encode())
            except Exception as error:
                if not stop.is_set():
                    failures.append(error)

        broker_thread = threading.Thread(target=broker, daemon=True)
        broker_thread.start()
        with tempfile.TemporaryDirectory() as directory:
            lab = Lab(Path(directory) / 'cloud.sqlite3')
            factory = lambda *args: BemfaTransport(*args, address=listener.getsockname(), retry_base=0.02)
            try:
                with patch('engine.BemfaTransport', side_effect=factory):
                    lab.configure_cloud('testkey123', 'cltelemetry004', 'clcontrol003')
                deadline = time.monotonic() + 3
                while not lab.transport.connected and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(lab.transport.connected)
                lab.start()
                lab.issue_command('ENV-002', 'fan_on', 'cloud-roundtrip')
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    command = lab.db.command('cloud-roundtrip')
                    if command['status'] == 'acknowledged' and all(lab.db.history(device) for device in lab.devices):
                        break
                    time.sleep(0.02)
                self.assertEqual(lab.db.command('cloud-roundtrip')['status'], 'acknowledged')
                self.assertTrue(lab.models['ENV-002']['fan'])
                self.assertFalse(lab.models['ENV-001']['fan'])
                self.assertEqual({lab.db.history(device)[-1]['mode'] for device in lab.devices}, {'cloud'})
                self.assertFalse(failures)
            finally:
                stop.set()
                lab.close()
        listener.close()
        broker_thread.join(timeout=3)


class HTTPTests(unittest.TestCase):
    def test_api_state_command_export_and_validation(self):
        with tempfile.TemporaryDirectory() as directory:
            server = create_server(0, Path(directory) / 'test.sqlite3', start_devices=False)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            origin = 'http://127.0.0.1:' + str(server.server_port)

            def post(path, payload, request_origin=origin):
                request = Request(origin + path, json.dumps(payload).encode(),
                                  {'Content-Type': 'application/json', 'Origin': request_origin})
                with urlopen(request, timeout=3) as response:
                    return json.load(response)

            try:
                post('/api/device', {'device_id': 'ENV-002', 'changes': {'threshold': 25}})
                post('/api/command', {'device_id': 'ENV-001', 'action': 'fan_on', 'id': 'http-id'})
                same = post('/api/command', {'device_id': 'ENV-001', 'action': 'fan_on', 'id': 'http-id'})
                self.assertEqual(same['command']['id'], 'http-id')
                with urlopen(origin + '/api/state?device=ENV-002', timeout=3) as response:
                    self.assertEqual(json.load(response)['devices'][1]['threshold'], 25)
                with urlopen(origin + '/api/export?device=ENV-001', timeout=3) as response:
                    self.assertTrue(response.read().startswith(b'\xef\xbb\xbfdevice_id'))
                with self.assertRaises(HTTPError) as error:
                    post('/api/device', {'device_id': 'ENV-001', 'changes': {'threshold': 99}})
                self.assertEqual(error.exception.code, 400)
                with self.assertRaises(HTTPError) as error:
                    post('/api/disconnect', {}, 'https://foreign.example')
                self.assertEqual(error.exception.code, 403)
                for filename in ('/', '/app.js', '/style.css', '/preview.html'):
                    with urlopen(origin + filename, timeout=3) as response:
                        self.assertEqual(response.status, 200)
            finally:
                server.shutdown()
                server.server_close()
                server.lab.close()
                thread.join(timeout=3)


if __name__ == '__main__':
    unittest.main()
