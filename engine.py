"""Virtual devices, a local message bus, and a persistent command state machine."""
import heapq
import json
import math
import random
import re
import threading
import time
import uuid

from storage import Database
from transport import BemfaTransport


DEVICE_NAMES = ('实验室 A', '实验室 B', '设备间')
SCENARIOS = {'normal', 'hot', 'loss', 'offline'}
ACTIONS = {'fan_on', 'fan_off'}


class Lab:
    def __init__(self, database_path, clock=time.time, rng=None):
        self.db = Database(database_path)
        self.clock = clock
        self.rng = rng or random.Random()
        self.lock = threading.RLock()
        self.connection_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = None
        self.transport = None
        self.generation = 0
        self.mode = 'local'
        self.queue = []
        self.queue_sequence = 0
        self.next_sample = 0
        saved = {device['id']: device for device in self.db.load_devices()}
        self.devices = {}
        self.models = {}
        for index, name in enumerate(DEVICE_NAMES, 1):
            device_id = f'ENV-{index:03}'
            device = saved.get(device_id, {
                'id': device_id, 'name': name, 'auto': False, 'threshold': 30,
                'running': True, 'scenario': 'normal', 'fan': False,
            })
            self.devices[device_id] = device
            self.models[device_id] = {'temp': 26.0 + index, 'humidity': 53.0 + index * 3,
                                      'fan': device['fan']}
            self.db.save_device(device)
        self.db.event('CloudLink Lab 2.0 已启动，三个虚拟节点就绪')

    def start(self):
        self.thread = threading.Thread(target=self.run, daemon=True, name='virtual-devices')
        self.thread.start()

    def run(self):
        while not self.stop_event.wait(0.1):
            self.step()

    def event(self, message, category='info', device_id=None):
        self.db.event(message, category, device_id)

    def configure_cloud(self, uid, data_topic, control_topic):
        if not isinstance(uid, str) or not re.fullmatch(r'[A-Za-z0-9]{8,128}', uid):
            raise ValueError('私钥格式不正确')
        if not all(isinstance(topic, str) and re.fullmatch(r'[A-Za-z0-9_]{1,64}', topic)
                   for topic in (data_topic, control_topic)) or data_topic == control_topic:
            raise ValueError('请填写两个不同的主题名称')
        self.change_transport(uid, data_topic, control_topic)

    def change_transport(self, uid=None, data_topic=None, control_topic=None):
        with self.connection_lock:
            with self.lock:
                old_transport = self.transport
                self.transport = None
                self.generation += 1
                generation = self.generation
                self.mode = 'cloud' if uid else 'local'
                self.queue.clear()
                self.db.execute("UPDATE commands SET status='interrupted' WHERE status='pending'")
            if old_transport:
                old_transport.close()
            if uid:
                transport = BemfaTransport(
                    uid, data_topic, control_topic,
                    lambda channel, payload: self.receive(channel, payload, generation),
                    lambda message, category: self.connection_event(message, category, generation),
                )
                with self.lock:
                    self.transport = transport
                transport.start()
            self.event('已选择巴法云模式，正在建立连接' if uid else '已返回本地消息仿真')

    def connection_event(self, message, category, generation):
        with self.lock:
            if generation == self.generation:
                self.event(message, category)

    def configure_device(self, device_id, changes):
        with self.lock:
            if device_id not in self.devices:
                raise ValueError('设备不存在')
            allowed = {'auto', 'threshold', 'running', 'scenario'}
            if not changes or set(changes) - allowed:
                raise ValueError('设备设置无效')
            for key in ('auto', 'running'):
                if key in changes and type(changes[key]) is not bool:
                    raise ValueError('开关设置须为布尔值')
            if 'threshold' in changes:
                value = changes['threshold']
                if type(value) not in (int, float) or not math.isfinite(value) or not 20 <= value <= 40:
                    raise ValueError('阈值须在 20 到 40°C 之间')
            if 'scenario' in changes and changes['scenario'] not in SCENARIOS:
                raise ValueError('未知实验场景')
            self.devices[device_id].update(changes)
            self.db.save_device(self.devices[device_id])
            self.event('设备设置已更新', 'info', device_id)

    def issue_command(self, device_id, action, command_id=None, source='manual'):
        with self.lock:
            if device_id not in self.devices or action not in ACTIONS:
                raise ValueError('设备或控制动作无效')
            command_id = uuid.uuid4().hex if command_id is None else command_id
            if not isinstance(command_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', command_id):
                raise ValueError('指令编号格式无效')
            existing = self.db.command(command_id)
            if existing:
                if existing['device_id'] != device_id or existing['action'] != action:
                    raise ValueError('指令编号已用于其他操作')
                return existing
            device = self.devices[device_id]
            if source == 'manual' and device['auto']:
                raise ValueError('请先切换到手动模式')
            if not device['running']:
                raise ValueError('请先恢复设备运行')
            if self.db.rows("SELECT id FROM commands WHERE device_id=? AND status='pending'", (device_id,)):
                raise ValueError('该设备仍有等待确认的指令')
            now = self.clock()
            self.db.execute(
                'INSERT INTO commands(id,device_id,action,source,created_at,expires_at) VALUES (?,?,?,?,?,?)',
                (command_id, device_id, action, source, now, now + 5),
            )
            self.event('创建指令 ' + action + ' · ' + command_id[:8], 'control', device_id)
            return self.db.command(command_id)

    def transmit(self, channel, payload):
        device = self.devices.get(payload.get('device_id'))
        if not device:
            return False
        if self.mode == 'cloud':
            return bool(self.transport and self.transport.publish(channel, payload))
        if device['scenario'] == 'loss' and self.rng.random() < 0.45:
            return True
        self.queue_sequence += 1
        heapq.heappush(self.queue, (self.clock() + 0.12, self.queue_sequence, channel, payload.copy()))
        return True

    def receive(self, channel, payload, generation=None):
        with self.lock:
            if generation is not None and generation != self.generation:
                return
            device_id = payload.get('device_id', 'ENV-001')
            if not isinstance(device_id, str):
                return
            device = self.devices.get(device_id)
            if not device or not device['running'] or device['scenario'] == 'offline':
                return
            if self.mode == 'cloud' and device['scenario'] == 'loss' and self.rng.random() < 0.45:
                return
            if channel == 'control' and payload.get('kind') == 'command':
                self.execute_command(device_id, payload)
            elif channel == 'telemetry' and payload.get('kind') == 'ack':
                self.accept_ack(device_id, payload)
            elif channel == 'telemetry' and payload.get('kind') == 'telemetry':
                self.accept_sample(device_id, payload)

    def execute_command(self, device_id, payload):
        command_id = payload.get('id')
        if not isinstance(command_id, str):
            return
        command = self.db.command(command_id)
        now = self.clock()
        if not command or command['device_id'] != device_id or command['action'] != payload.get('action'):
            return
        if command['status'] != 'pending' or command['expires_at'] <= now:
            return
        # The persisted receipt makes retries idempotent: execute once, resend the same ack.
        if command['executed_at'] is None:
            fan = command['action'] == 'fan_on'
            device = self.devices[device_id]
            device['fan'] = fan
            with self.db.lock, self.db.connection:
                self.db.connection.execute(
                    'UPDATE commands SET executed_at=?,ack_fan=? WHERE id=? AND executed_at IS NULL',
                    (now, int(fan), command_id),
                )
                self.db.connection.execute(
                    'UPDATE devices SET settings=? WHERE id=?',
                    (json.dumps(device, ensure_ascii=False), device_id),
                )
            self.models[device_id]['fan'] = fan
            self.event('设备执行 ' + command['action'] + ' · ' + command_id[:8], 'control', device_id)
            command = self.db.command(command_id)
        self.transmit('telemetry', {'kind': 'ack', 'device_id': device_id,
                                  'id': command_id, 'fan': bool(command['ack_fan'])})

    def accept_ack(self, device_id, payload):
        if not isinstance(payload.get('id'), str) or type(payload.get('fan')) is not bool:
            return
        command = self.db.command(payload['id'])
        now = self.clock()
        if not command or command['device_id'] != device_id or command['status'] != 'pending':
            return
        if command['expires_at'] <= now or command['executed_at'] is None:
            return
        if payload['fan'] != bool(command['ack_fan']):
            return
        latency = round((now - command['created_at']) * 1000)
        self.db.execute(
            "UPDATE commands SET status='acknowledged',acknowledged_at=?,latency_ms=? WHERE id=?",
            (now, latency, command['id']),
        )
        self.event('设备已确认 · ' + str(latency) + ' ms · ' + command['id'][:8], 'control', device_id)

    def accept_sample(self, device_id, payload):
        temperature, humidity = payload.get('temp'), payload.get('humidity')
        sample_id = payload.get('sample_id')
        if not isinstance(sample_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', sample_id):
            return
        if any(type(value) not in (float, int) or not math.isfinite(value) for value in (temperature, humidity)):
            return
        if not -40 <= temperature <= 125 or not 0 <= humidity <= 100 or type(payload.get('fan')) is not bool:
            return
        previous = self.db.history(device_id, limit=1)
        sample = {**payload, 'device_id': device_id}
        self.db.add_sample(sample, self.mode)
        threshold = self.devices[device_id]['threshold']
        if temperature >= threshold and (not previous or previous[-1]['temperature'] < threshold):
            self.event('温度达到报警阈值 ' + str(threshold) + '°C', 'alarm', device_id)

    def step(self):
        with self.lock:
            now = self.clock()
            while self.queue and self.queue[0][0] <= now:
                _, _, channel, payload = heapq.heappop(self.queue)
                self.receive(channel, payload)
            for command in self.db.rows("SELECT * FROM commands WHERE status='pending'"):
                if command['expires_at'] <= now:
                    self.db.execute("UPDATE commands SET status='timeout' WHERE id=?", (command['id'],))
                    self.event('控制超时，5 秒内未收到确认 · ' + command['id'][:8], 'error', command['device_id'])
                elif command['attempts'] < 3 and (command['attempts'] == 0 or now - command['last_attempt'] >= 1):
                    self.db.execute('UPDATE commands SET attempts=attempts+1,last_attempt=? WHERE id=?',
                                    (now, command['id']))
                    self.transmit('control', {'kind': 'command', 'device_id': command['device_id'],
                                             'id': command['id'], 'action': command['action']})
            if now < self.next_sample:
                return
            self.next_sample = now + 3
            for device_id, device in self.devices.items():
                if not device['running'] or device['scenario'] == 'offline':
                    continue
                model = self.models[device_id]
                target = (37 if device['scenario'] == 'hot' else 28) - (8 if model['fan'] else 0)
                model['temp'] += (target - model['temp']) * 0.16 + self.rng.uniform(-0.14, 0.14)
                model['humidity'] = min(85, max(25, model['humidity'] + self.rng.uniform(-0.7, 0.7)))
                self.transmit('telemetry', {'kind': 'telemetry', 'sample_id': uuid.uuid4().hex,
                                          'device_id': device_id, 'temp': round(model['temp'], 1),
                                          'humidity': round(model['humidity'], 1), 'fan': model['fan']})
                if device['auto']:
                    self.auto_control(device_id, device, model, now)

    def auto_control(self, device_id, device, model, now):
        recent = self.db.rows('SELECT * FROM commands WHERE device_id=? ORDER BY created_at DESC LIMIT 1', (device_id,))
        if recent and (recent[0]['status'] == 'pending' or recent[0]['created_at'] > now - 8):
            return
        if model['temp'] >= device['threshold'] and not model['fan']:
            self.issue_command(device_id, 'fan_on', source='auto')
        elif model['temp'] < device['threshold'] - 1 and model['fan']:
            self.issue_command(device_id, 'fan_off', source='auto')

    def snapshot(self, selected='ENV-001', limit=120, after=0):
        with self.lock:
            if selected not in self.devices:
                raise ValueError('设备不存在')
            devices = []
            now = self.clock()
            for device in self.devices.values():
                latest = self.db.history(device['id'], limit=1)
                sample = latest[-1] if latest else None
                fresh = bool(sample and now - sample['received_at'] < 10 and sample['mode'] == self.mode)
                online = fresh and device['running'] and device['scenario'] != 'offline'
                if self.mode == 'cloud':
                    online = online and bool(self.transport and self.transport.connected)
                devices.append({**device, 'latest': sample, 'online': bool(online)})
            commands = self.db.rows('SELECT * FROM commands WHERE device_id=? ORDER BY created_at DESC LIMIT 20', (selected,))
            events = self.db.rows('SELECT * FROM events WHERE device_id=? OR device_id IS NULL ORDER BY id DESC LIMIT 100', (selected,))
            stats = self.db.rows(
                'SELECT COUNT(*) AS total, '
                "COALESCE(SUM(status='acknowledged'),0) AS acknowledged FROM commands WHERE device_id=?", (selected,),
            )[0]
            stats['samples'] = self.db.rows('SELECT COUNT(*) AS n FROM telemetry WHERE device_id=?', (selected,))[0]['n']
            stats['alarms'] = self.db.rows("SELECT COUNT(*) AS n FROM events WHERE device_id=? AND category='alarm'", (selected,))[0]['n']
            connection = self.transport.snapshot() if self.transport else {'connected': False, 'state': 'local', 'reconnections': 0}
            return {'version': '2.0.0', 'mode': self.mode, 'devices': devices,
                    'selected': selected, 'connection': connection, 'commands': commands,
                    'history': self.db.history(selected, limit, after), 'events': events, 'stats': stats}

    def close(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=5)
        with self.connection_lock:
            if self.transport:
                self.transport.close()
        self.db.close()
