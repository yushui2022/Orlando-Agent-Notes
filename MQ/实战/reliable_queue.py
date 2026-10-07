"""A failure-injection teaching model, not a production message broker.

Python 3.11+, standard library only. Application and broker use separate SQLite
files so publishing and business commits cannot accidentally share a transaction.
"""

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid


@contextmanager
def transaction(path):
    connection = sqlite3.connect(path, isolation_level=None, timeout=10)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("BEGIN IMMEDIATE")
        yield connection
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


def crash(selected, point):
    if selected == point:
        print(f"CRASH: {point}; exit=86", flush=True)
        os._exit(86)  # Deliberately bypass finally/rollback: simulate process death.


class Lab:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.app = self.directory / "app.sqlite"
        self.broker = self.directory / "broker.sqlite"
        with transaction(self.app) as db:
            db.execute("""CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY, title TEXT NOT NULL,
                event_id TEXT UNIQUE NOT NULL, state TEXT NOT NULL)""")
            db.execute("""CREATE TABLE IF NOT EXISTS outbox (
                event_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
                sent INTEGER NOT NULL DEFAULT 0)""")
            db.execute("""CREATE TABLE IF NOT EXISTS inbox (
                consumer TEXT NOT NULL, event_id TEXT NOT NULL,
                PRIMARY KEY (consumer, event_id))""")
            db.execute("""CREATE TABLE IF NOT EXISTS results (
                task_id TEXT PRIMARY KEY, result TEXT NOT NULL)""")
        with transaction(self.broker) as db:
            db.execute("""CREATE TABLE IF NOT EXISTS deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL, task_id TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'ready',
                available_at REAL NOT NULL DEFAULT 0,
                lease_until REAL NOT NULL DEFAULT 0,
                token TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT)""")

    def submit(self, task_id, title):
        with transaction(self.app) as db:
            previous = db.execute(
                "SELECT * FROM tasks WHERE task_id=?", (task_id,)
            ).fetchone()
            if previous:
                if previous["title"] != title:
                    raise ValueError("same task_id with different title")
                return previous["event_id"]
            event_id = str(uuid.uuid4())
            db.execute("INSERT INTO tasks VALUES (?, ?, ?, 'queued')",
                       (task_id, title, event_id))
            db.execute("INSERT INTO outbox VALUES (?, ?, 0)",
                       (event_id, task_id))
            return event_id

    def relay(self, failure=None):
        # Single relay in this lab. Duplicate publication is intentionally legal.
        with transaction(self.app) as db:
            pending = db.execute(
                "SELECT * FROM outbox WHERE sent=0 ORDER BY rowid"
            ).fetchall()
        for message in pending:
            with transaction(self.broker) as db:
                db.execute("INSERT INTO deliveries (event_id, task_id) VALUES (?, ?)",
                           (message["event_id"], message["task_id"]))
            crash(failure, "after-publish")
            with transaction(self.app) as db:
                db.execute("UPDATE outbox SET sent=1 WHERE event_id=?",
                           (message["event_id"],))
        return len(pending)

    def claim(self, now, lease_seconds=5):
        with transaction(self.broker) as db:
            row = db.execute("""SELECT * FROM deliveries
                WHERE (state='ready' AND available_at<=?)
                   OR (state='inflight' AND lease_until<=?)
                ORDER BY id LIMIT 1""", (now, now)).fetchone()
            if row is None:
                return None
            token = str(uuid.uuid4())
            db.execute("""UPDATE deliveries SET state='inflight', token=?,
                lease_until=?, attempts=attempts+1 WHERE id=?""",
                       (token, now + lease_seconds, row["id"]))
            return dict(db.execute("SELECT * FROM deliveries WHERE id=?",
                                   (row["id"],)).fetchone())

    def ack(self, message):
        with transaction(self.broker) as db:
            count = db.execute("""UPDATE deliveries SET state='acked'
                WHERE id=? AND token=? AND state='inflight'""",
                               (message["id"], message["token"])).rowcount
            return count == 1

    def fail(self, message, now, max_attempts):
        exhausted = message["attempts"] >= max_attempts
        delay = min(2 ** min(message["attempts"] - 1, 6), 60)
        with transaction(self.broker) as db:
            changed = db.execute("""UPDATE deliveries
                SET state=?, available_at=?, last_error='simulated failure'
                WHERE id=? AND token=? AND state='inflight'""",
                ("dead" if exhausted else "ready", now + delay,
                 message["id"], message["token"])).rowcount
        if not changed:
            return "stale receipt"
        if exhausted:
            with transaction(self.app) as db:
                db.execute("UPDATE tasks SET state='failed' WHERE task_id=? AND state='queued'",
                           (message["task_id"],))
        return "dead" if exhausted else "retry"

    def work(self, now, failure=None, simulate_failure=False, max_attempts=3):
        message = self.claim(now)
        if message is None:
            return "empty"
        crash(failure, "after-claim")
        # Check idempotency before a simulated operation failure.
        with transaction(self.app) as db:
            done = db.execute("SELECT 1 FROM inbox WHERE consumer='report' AND event_id=?",
                              (message["event_id"],)).fetchone()
        if done:
            return "duplicate" if self.ack(message) else "stale receipt"
        if simulate_failure:
            return self.fail(message, now, max_attempts)
        with transaction(self.app) as db:
            inserted = db.execute("INSERT OR IGNORE INTO inbox VALUES ('report', ?)",
                                  (message["event_id"],)).rowcount
            if inserted:
                task = db.execute("SELECT title FROM tasks WHERE task_id=?",
                                  (message["task_id"],)).fetchone()
                if task is None:
                    raise ValueError("unknown task")
                # A deterministic local result; no LLM, network, or sandbox.
                db.execute("INSERT INTO results VALUES (?, ?)",
                           (message["task_id"], f"report:{task['title']}"))
                db.execute("UPDATE tasks SET state='succeeded' WHERE task_id=?",
                           (message["task_id"],))
            crash(failure, "before-commit")
        crash(failure, "after-commit")
        if not self.ack(message):
            return "stale receipt"
        return "succeeded" if inserted else "duplicate"

    def status(self):
        snapshot = {}
        with transaction(self.app) as db:
            for table in ("tasks", "outbox", "inbox", "results"):
                snapshot[table] = [dict(row) for row in db.execute(f"SELECT * FROM {table}")]
        with transaction(self.broker) as db:
            snapshot["deliveries"] = [dict(row) for row in db.execute("SELECT * FROM deliveries")]
        return snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=Path(__file__).parent / ".data" / "local")
    parser.add_argument("--now", type=float, help="logical seconds for deterministic experiments")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    submit = sub.add_parser("submit")
    submit.add_argument("task_id")
    submit.add_argument("title")
    relay = sub.add_parser("relay")
    relay.add_argument("--crash", choices=["after-publish"])
    work = sub.add_parser("work")
    work.add_argument("--crash", choices=["after-claim", "before-commit", "after-commit"])
    work.add_argument("--fail", action="store_true")
    sub.add_parser("status")
    args = parser.parse_args()
    lab = Lab(args.data)
    if args.command == "submit":
        result = lab.submit(args.task_id, args.title)
    elif args.command == "relay":
        result = lab.relay(args.crash)
    elif args.command == "work":
        now = time.time() if args.now is None else args.now
        result = lab.work(now, args.crash, args.fail)
    elif args.command == "status":
        result = lab.status()
    else:
        result = str(args.data)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
