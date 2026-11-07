"""SQLite persistence for device configuration, telemetry, and command receipts."""
import json
import sqlite3
import threading
import time
from pathlib import Path


class Database:
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.connection = sqlite3.connect(path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        with self.lock, self.connection:
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.executescript("""
                CREATE TABLE IF NOT EXISTS devices (
                    id TEXT PRIMARY KEY, settings TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS telemetry (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    sample_id TEXT UNIQUE NOT NULL, device_id TEXT NOT NULL,
                    received_at REAL NOT NULL, temperature REAL NOT NULL,
                    humidity REAL NOT NULL, fan INTEGER NOT NULL, mode TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS telemetry_device_time
                    ON telemetry(device_id, received_at);
                CREATE TABLE IF NOT EXISTS commands (
                    id TEXT PRIMARY KEY, device_id TEXT NOT NULL,
                    action TEXT NOT NULL, source TEXT NOT NULL,
                    created_at REAL NOT NULL, expires_at REAL NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_attempt REAL NOT NULL DEFAULT 0,
                    executed_at REAL, ack_fan INTEGER,
                    status TEXT NOT NULL DEFAULT 'pending',
                    acknowledged_at REAL, latency_ms INTEGER
                );
                CREATE INDEX IF NOT EXISTS commands_device_time
                    ON commands(device_id, created_at);
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at REAL NOT NULL, device_id TEXT,
                    category TEXT NOT NULL, message TEXT NOT NULL
                );
            """)
            # A restarted process must never replay previously pending controls.
            self.connection.execute(
                "UPDATE commands SET status='interrupted' WHERE status='pending'"
            )

    def execute(self, sql, parameters=()):
        with self.lock, self.connection:
            return self.connection.execute(sql, parameters)

    def rows(self, sql, parameters=()):
        with self.lock:
            return [dict(row) for row in self.connection.execute(sql, parameters)]

    def save_device(self, device):
        self.execute(
            "INSERT INTO devices VALUES (?, ?) ON CONFLICT(id) "
            "DO UPDATE SET settings=excluded.settings",
            (device['id'], json.dumps(device, ensure_ascii=False)),
        )

    def load_devices(self):
        return [json.loads(row['settings']) for row in self.rows("SELECT * FROM devices ORDER BY id")]

    def event(self, message, category='info', device_id=None):
        self.execute("INSERT INTO events(created_at,device_id,category,message) VALUES (?,?,?,?)",
                     (time.time(), device_id, category, message))

    def add_sample(self, sample, mode):
        self.execute(
            "INSERT OR IGNORE INTO telemetry "
            "(sample_id,device_id,received_at,temperature,humidity,fan,mode) "
            "VALUES (?,?,?,?,?,?,?)",
            (sample['sample_id'], sample['device_id'], time.time(), sample['temp'],
             sample['humidity'], int(sample['fan']), mode),
        )

    def history(self, device_id, limit=120, after=0):
        rows = self.rows(
            "SELECT * FROM telemetry WHERE device_id=? AND received_at>=? "
            "ORDER BY sequence DESC LIMIT ?", (device_id, after, limit),
        )
        return list(reversed(rows))

    def command(self, command_id):
        rows = self.rows("SELECT * FROM commands WHERE id=?", (command_id,))
        return rows[0] if rows else None

    def close(self):
        with self.lock:
            self.connection.close()
