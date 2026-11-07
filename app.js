'use strict';

const element = id => document.getElementById(id);
const formatTime = timestamp => new Date(timestamp * 1000).toLocaleString('zh-CN', {hour12: false});
const statusNames = {
  pending: '等待确认', acknowledged: '已确认', timeout: '已超时', interrupted: '已中断'
};
const scenarioHints = {
  normal: '正常采样。风扇开启后，当前节点的虚拟温度逐渐下降。',
  hot: '增强当前节点的虚拟热源，可用自动模式观察阈值联动。',
  loss: '随机丢弃约 45% 的消息，观察同一指令的重试与回执。',
  offline: '当前节点停止收发消息。其他节点继续运行，发往本节点的控制会超时。'
};
let selectedDevice = 'ENV-001';
let snapshot = null;
let polling = false;
let requestInProgress = false;
let toastTimer;

function toast(message) {
  element('toast').textContent = message;
  element('toast').hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { element('toast').hidden = true; }, 3500);
}

async function request(path, body) {
  const response = await fetch(path, {
    method: body ? 'POST' : 'GET',
    headers: body ? {'Content-Type': 'application/json'} : {},
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(12000)
  });
  const result = await response.json();
  if (!response.ok || result.ok === false) {
    throw new Error(result.error || '操作未完成，请检查本机服务');
  }
  return result;
}

function historyAfter() {
  const seconds = Number(element('timeRange').value);
  return seconds ? Math.floor(Date.now() / 1000) - seconds : 0;
}

async function poll() {
  if (polling) return;
  polling = true;
  const requestedDevice = selectedDevice;
  const range = element('timeRange').value;
  try {
    const query = new URLSearchParams({device: requestedDevice, limit: '120', after: String(historyAfter())});
    const result = await request('/api/state?' + query);
    if (requestedDevice !== selectedDevice || range !== element('timeRange').value) return;
    snapshot = result;
    element('serviceError').hidden = true;
    render();
  } catch (error) {
    element('serviceError').hidden = false;
    element('connectionBadge').textContent = '本机服务未连接';
    element('fanButton').disabled = true;
    element('cloudStatus').textContent = '本机服务不可用，请检查启动窗口';
  } finally {
    polling = false;
  }
}

async function mutate(path, body) {
  if (requestInProgress) return;
  requestInProgress = true;
  try {
    await request(path, body);
    await poll();
  } catch (error) {
    toast(error.message);
  } finally {
    requestInProgress = false;
  }
}

function currentDevice() {
  if (snapshot?.selected !== selectedDevice) return null;
  return snapshot.devices.find(device => device.id === selectedDevice);
}

async function configureDevice(changes) {
  if (!currentDevice()) return;
  await mutate('/api/device', {device_id: selectedDevice, changes});
}

function selectDevice(deviceId) {
  selectedDevice = deviceId;
  element('deviceSelect').value = deviceId;
  element('fanButton').disabled = true;
  element('pauseButton').disabled = true;
  poll();
}

function createCell(row, value, className) {
  const cell = document.createElement('td');
  cell.textContent = value;
  if (className) cell.className = className;
  row.append(cell);
}

function renderDevices() {
  const container = element('deviceCards');
  if (!container.children.length) {
    for (const device of snapshot.devices) {
      const button = document.createElement('button');
      button.className = 'device-card';
      button.dataset.device = device.id;
      button.innerHTML = '<div class="device-top"><span></span><span></span></div><strong></strong><div class="device-reading"></div>';
      button.onclick = () => selectDevice(device.id);
      container.append(button);
      const option = document.createElement('option');
      option.value = device.id;
      option.textContent = device.name + ' · ' + device.id;
      element('deviceSelect').append(option);
    }
  }
  for (const device of snapshot.devices) {
    const button = container.querySelector('[data-device="' + device.id + '"]');
    button.classList.toggle('selected', device.id === selectedDevice);
    button.setAttribute('aria-pressed', String(device.id === selectedDevice));
    const labels = button.querySelectorAll('.device-top span');
    labels[0].textContent = device.id;
    labels[1].textContent = device.online ? '● 在线' : '○ 等待 / 离线';
    labels[1].className = device.online ? 'online' : 'offline';
    button.querySelector('strong').textContent = device.name;
    button.querySelector('.device-reading').textContent = device.latest ? device.latest.temperature.toFixed(1) + ' °C' : '— °C';
  }
}

function renderConnection() {
  const cloud = snapshot.mode === 'cloud';
  const connection = snapshot.connection;
  const connected = connection.connected;
  const state = cloud ? (connected ? '巴法云 · 真实传输' : connection.state === 'reconnecting' ? '巴法云 · 自动重连中' : '巴法云 · 正在连接') : '本地仿真 · SQLite 已启用';
  element('connectionBadge').textContent = state;
  element('connectionBadge').classList.toggle('live', cloud && connected);
  element('serviceNotice').textContent = cloud
    ? '设备和环境由 Python 模拟，数据与指令通过巴法云 TCP 服务传输。已接收的数据写入本机数据库。'
    : '本地模式使用内存消息队列，三个虚拟节点独立运行。采样和控制记录保存到本机 SQLite。';
  element('cloudStatus').textContent = state;
  const retryTime = connection.next_retry_at ? '，下次尝试 ' + formatTime(connection.next_retry_at) : '';
  element('reconnectStats').textContent = '本次连接重连次数 ' + connection.reconnections + retryTime;
  if (connection.topics) {
    for (const [id, value] of [['dataTopic', connection.topics.telemetry], ['controlTopic', connection.topics.control]]) {
      if (document.activeElement !== element(id)) element(id).value = value;
    }
  }
}

function renderEvents() {
  for (const [id, recent] of [['recentEvents', true], ['allEvents', false]]) {
    const container = element(id);
    container.replaceChildren();
    const filter = element('eventFilter').value;
    const events = recent ? snapshot.events.slice(0, 5) : snapshot.events.filter(event => filter === 'all' || event.category === filter);
    for (const event of events) {
      const row = document.createElement('div');
      row.className = 'logrow';
      const time = document.createElement('time');
      time.textContent = new Date(event.created_at * 1000).toLocaleTimeString('zh-CN', {hour12: false});
      const message = document.createElement('span');
      message.textContent = event.message;
      if (['error', 'alarm'].includes(event.category)) message.className = 'alarm';
      row.append(time, message);
      container.append(row);
    }
    if (!events.length) container.textContent = '暂无符合条件的事件';
  }
}

function renderRecords() {
  const rows = element('historyRows');
  rows.replaceChildren();
  for (const sample of [...snapshot.history].reverse()) {
    const row = document.createElement('tr');
    for (const value of [formatTime(sample.received_at), sample.temperature.toFixed(1), sample.humidity.toFixed(1), sample.fan ? '开启' : '关闭', sample.mode === 'cloud' ? '巴法云 TCP' : '本地仿真']) {
      createCell(row, value);
    }
    rows.append(row);
  }
  if (!snapshot.history.length) {
    const row = document.createElement('tr');
    createCell(row, '该时间范围暂无采样');
    rows.append(row);
  }
  const commands = element('commandRows');
  commands.replaceChildren();
  for (const command of snapshot.commands) {
    const row = document.createElement('tr');
    row.title = command.id;
    for (const value of [command.id.slice(0, 8), command.action === 'fan_on' ? '开启风扇' : '关闭风扇', command.source === 'auto' ? '自动' : '手动', command.attempts, statusNames[command.status] || command.status, command.latency_ms === null ? '—' : command.latency_ms + ' ms']) {
      createCell(row, value);
    }
    commands.append(row);
  }
  element('sampleTotal').textContent = snapshot.stats.samples;
  element('commandTotal').textContent = snapshot.stats.acknowledged + ' / ' + snapshot.stats.total;
  element('alarmTotal').textContent = snapshot.stats.alarms;
}

function render() {
  const device = currentDevice();
  if (!device) return;
  renderDevices();
  renderConnection();
  element('pauseButton').disabled = false;
  const latest = device.latest;
  const lastCommand = snapshot.commands[0];
  const acknowledged = snapshot.commands.find(command => command.status === 'acknowledged');
  const pending = snapshot.commands.some(command => command.status === 'pending');
  const receivedFan = latest ? Boolean(latest.fan) : false;
  const displayedFan = lastCommand?.status === 'acknowledged' && (!latest || lastCommand.acknowledged_at > latest.received_at)
    ? Boolean(lastCommand.ack_fan) : receivedFan;
  element('temperature').textContent = latest ? latest.temperature.toFixed(1) : '—';
  element('humidity').textContent = latest ? latest.humidity.toFixed(1) : '—';
  element('temperatureHint').textContent = latest && latest.temperature >= device.threshold ? '超温报警' : device.name + ' · 虚拟环境节点';
  element('temperatureHint').classList.toggle('alarm', Boolean(latest && latest.temperature >= device.threshold));
  element('latency').textContent = acknowledged?.latency_ms ?? '—';
  element('deviceStatus').textContent = !device.running ? '已暂停' : device.online ? '在线运行' : device.scenario === 'offline' ? '设备离线' : '等待新数据';
  element('lastSeen').textContent = latest ? '最后接收 ' + formatTime(latest.received_at) : '尚未收到采样';
  element('controlTitle').textContent = device.name + ' · 控制';
  element('fanState').textContent = latest || acknowledged ? (displayedFan ? '风扇开启' : '风扇关闭') : '等待设备状态';
  element('fanButton').classList.toggle('on', displayedFan);
  element('fanButton').setAttribute('aria-checked', String(displayedFan));
  element('fanButton').disabled = device.auto || pending || !device.running;
  element('manualButton').classList.toggle('sel', !device.auto);
  element('autoButton').classList.toggle('sel', device.auto);
  element('thresholdValue').textContent = device.threshold;
  if (document.activeElement !== element('threshold')) element('threshold').value = device.threshold;
  element('pauseButton').textContent = device.running ? '暂停当前设备' : '恢复当前设备';
  element('commandStatus').textContent = lastCommand
    ? (lastCommand.status === 'acknowledged' ? '设备已确认 · ' + (lastCommand.ack_fan ? '风扇开启' : '风扇关闭') : statusNames[lastCommand.status]) + ' · 尝试 ' + lastCommand.attempts + '/3'
    : '等待操作';
  document.querySelectorAll('[data-scenario]').forEach(button => button.classList.toggle('selected', button.dataset.scenario === device.scenario));
  element('scenarioHint').textContent = scenarioHints[device.scenario];
  renderRecords();
  renderEvents();
  drawChart();
}

function drawChart() {
  if (!snapshot || element('overview').hidden) return;
  const canvas = element('chart');
  const box = canvas.getBoundingClientRect();
  const ratio = window.devicePixelRatio || 1;
  canvas.width = box.width * ratio;
  canvas.height = box.height * ratio;
  const context = canvas.getContext('2d');
  context.scale(ratio, ratio);
  const samples = snapshot.history;
  const threshold = currentDevice().threshold;
  const minimum = Math.floor(Math.min(threshold, 24, ...samples.map(sample => sample.temperature)) / 5) * 5 - 1;
  const maximum = Math.ceil(Math.max(threshold, 32, ...samples.map(sample => sample.temperature)) / 5) * 5 + 1;
  const left = 35;
  const bottom = box.height - 20;
  const y = value => 12 + (maximum - value) / (maximum - minimum) * (bottom - 12);
  const firstTime = samples[0]?.received_at || 0;
  const duration = Math.max(1, (samples.at(-1)?.received_at || 0) - firstTime);
  const x = sample => left + (sample.received_at - firstTime) / duration * (box.width - left - 12);
  context.font = '10px sans-serif';
  for (let index = 0; index <= 4; index++) {
    const value = minimum + (maximum - minimum) * index / 4;
    context.strokeStyle = '#e9eeeb';
    context.beginPath(); context.moveTo(left, y(value)); context.lineTo(box.width - 12, y(value)); context.stroke();
    context.fillStyle = '#81968b'; context.fillText(value.toFixed(0) + '°', 0, y(value) + 3);
  }
  context.setLineDash([4, 4]);
  context.strokeStyle = '#c69f65';
  context.beginPath(); context.moveTo(left, y(threshold)); context.lineTo(box.width - 12, y(threshold)); context.stroke();
  context.setLineDash([]);
  context.strokeStyle = '#087f69'; context.lineWidth = 2.5;
  context.beginPath();
  samples.forEach((sample, index) => {
    if (index) context.lineTo(x(sample), y(sample.temperature));
    else context.moveTo(x(sample), y(sample.temperature));
  });
  context.stroke();
  for (const sample of samples) {
    context.fillStyle = '#087f69'; context.beginPath(); context.arc(x(sample), y(sample.temperature), 2, 0, Math.PI * 2); context.fill();
  }
  element('chartStart').textContent = samples.length ? formatTime(firstTime) : '等待采样';
  element('chartEnd').textContent = samples.length ? formatTime(samples.at(-1).received_at) : '—';
}

for (const button of document.querySelectorAll('[data-page]')) {
  button.onclick = () => {
    document.querySelectorAll('.panel').forEach(panel => { panel.hidden = panel.id !== button.dataset.page; });
    document.querySelectorAll('[data-page]').forEach(item => item.classList.toggle('active', item === button));
    drawChart();
  };
}
element('deviceSelect').onchange = event => selectDevice(event.target.value);
element('timeRange').onchange = poll;
element('eventFilter').onchange = () => { if (snapshot) renderEvents(); };
element('pauseButton').onclick = () => { if (currentDevice()) configureDevice({running: !currentDevice().running}); };
element('manualButton').onclick = () => configureDevice({auto: false});
element('autoButton').onclick = () => configureDevice({auto: true});
element('threshold').oninput = event => { element('thresholdValue').textContent = event.target.value; };
element('threshold').onchange = event => configureDevice({threshold: Number(event.target.value)});
element('scenarioButtons').onclick = event => {
  const button = event.target.closest('[data-scenario]');
  if (button) configureDevice({scenario: button.dataset.scenario});
};
element('fanButton').onclick = async () => {
  if (!currentDevice() || requestInProgress) return;
  requestInProgress = true;
  element('fanButton').disabled = true;
  const command = {id: crypto.randomUUID(), device_id: selectedDevice, action: element('fanButton').getAttribute('aria-checked') === 'true' ? 'fan_off' : 'fan_on'};
  try {
    try {
      await request('/api/command', command);
    } catch (error) {
      // A lost HTTP response may follow a successful insert. Reuse the same identifier.
      await request('/api/command', command);
    }
    await poll();
  } catch (error) {
    toast(error.message);
  } finally {
    requestInProgress = false;
    if (snapshot) render();
  }
};
element('connectionForm').onsubmit = async event => {
  event.preventDefault();
  if (requestInProgress) return;
  element('connectButton').disabled = true;
  await mutate('/api/connect', {uid: element('uid').value.trim(), data: element('dataTopic').value.trim(), control: element('controlTopic').value.trim()});
  element('uid').value = '';
  element('connectButton').disabled = false;
};
element('disconnectButton').onclick = () => mutate('/api/disconnect', {});
element('exportButton').onclick = () => {
  const query = new URLSearchParams({device: selectedDevice, after: String(historyAfter())});
  const link = document.createElement('a');
  link.href = '/api/export?' + query;
  link.download = selectedDevice + '.csv';
  link.click();
};
window.addEventListener('resize', drawChart);
setInterval(() => { element('clock').textContent = new Date().toLocaleString('zh-CN', {hour12: false}); }, 1000);
setInterval(poll, 1000);
poll();
