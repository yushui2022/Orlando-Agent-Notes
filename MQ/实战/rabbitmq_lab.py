"""Real RabbitMQ work-queue experiment; requires a running broker and pika.

No LLM or sandbox is invoked. SQLite records the simulated report result.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path

from reliable_queue import crash, transaction

EXCHANGE = "mq.course.tasks.v1"
QUEUE = "mq.course.report.v1"
DLX = "mq.course.dead.v1"
DLQ = "mq.course.report.dead.v1"


def record_result(path, message, failure=None):
    if not isinstance(message, dict) or message.get("schema_version") != 1:
        raise ValueError("unsupported envelope")
    if message.get("type") != "ReportRequested":
        raise ValueError("unsupported event type")
    for name in ("event_id", "task_id", "title"):
        if not isinstance(message.get(name), str) or not message[name]:
            raise ValueError(f"invalid {name}")
    if message.get("poison"):
        raise ValueError("simulated permanent failure")
    digest = hashlib.sha256(json.dumps(message, sort_keys=True).encode()).hexdigest()
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with transaction(path) as db:
        db.execute("CREATE TABLE IF NOT EXISTS inbox (event_id TEXT PRIMARY KEY, digest TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS results (task_id TEXT PRIMARY KEY, title TEXT NOT NULL)")
        previous = db.execute("SELECT digest FROM inbox WHERE event_id=?",
                              (message["event_id"],)).fetchone()
        if previous:
            if previous["digest"] != digest:
                raise ValueError("event_id reused with a different payload")
            return "duplicate"
        result = db.execute("SELECT title FROM results WHERE task_id=?",
                            (message["task_id"],)).fetchone()
        if result and result["title"] != message["title"]:
            raise ValueError("task_id reused with a different title")
        if not result:
            db.execute("INSERT INTO results VALUES (?, ?)",
                       (message["task_id"], message["title"]))
        db.execute("INSERT INTO inbox VALUES (?, ?)", (message["event_id"], digest))
        crash(failure, "before-commit")
    crash(failure, "after-commit")
    return "duplicate task" if result else "succeeded"


def topology(channel):
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(exchange=DLX, exchange_type="direct", durable=True)
    channel.queue_declare(queue=DLQ, durable=True, arguments={"x-queue-type": "quorum"})
    channel.queue_bind(queue=DLQ, exchange=DLX, routing_key="failed")
    channel.queue_declare(queue=QUEUE, durable=True, arguments={
        "x-queue-type": "quorum", "x-dead-letter-exchange": DLX,
        "x-dead-letter-routing-key": "failed"})
    channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="report")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, default=Path(__file__).parent / ".data" / "rabbit.sqlite")
    sub = parser.add_subparsers(dest="command", required=True)
    pub = sub.add_parser("publish")
    pub.add_argument("task_id")
    pub.add_argument("title")
    pub.add_argument("--poison", action="store_true")
    consumer = sub.add_parser("consume")
    consumer.add_argument("--crash", choices=["before-commit", "after-commit"])
    consumer.add_argument("--delay", type=float, default=0)
    sub.add_parser("inspect")
    args = parser.parse_args()
    if args.command == "inspect":
        if not args.state.exists():
            print("no local result database yet")
            return
        with transaction(args.state) as db:
            tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            for table in ("inbox", "results"):
                rows = [dict(row) for row in db.execute(f"SELECT * FROM {table}")] if table in tables else []
                print(table, json.dumps(rows, ensure_ascii=False))
        return
    try:
        import pika
    except ImportError:
        parser.error("install requirements.txt in a virtual environment first")
    parameters = pika.URLParameters(os.environ.get("AMQP_URL", "amqp://guest:guest@127.0.0.1:5672/%2F"))
    parameters.heartbeat = 30
    parameters.blocked_connection_timeout = 30
    connection = pika.BlockingConnection(parameters)
    try:
        channel = connection.channel()
        topology(channel)
        if args.command == "publish":
            channel.confirm_delivery()
            message = {"schema_version": 1, "type": "ReportRequested",
                       "event_id": f"report:{args.task_id}", "task_id": args.task_id,
                       "title": args.title, "poison": args.poison}
            channel.basic_publish(exchange=EXCHANGE, routing_key="report", mandatory=True,
                                  body=json.dumps(message).encode(),
                                  properties=pika.BasicProperties(delivery_mode=2,
                                      content_type="application/json", message_id=message["event_id"]))
            print("confirmed:", message["event_id"])
        else:
            if args.delay < 0:
                parser.error("--delay must be non-negative")
            channel.basic_qos(prefetch_count=1)

            def handle(ch, method, properties, body):
                try:
                    # Maintain Pika heartbeats during the simulated delay.
                    connection.sleep(args.delay)
                    status = record_result(args.state, json.loads(body), args.crash)
                except (ValueError, UnicodeDecodeError) as error:
                    print("rejected:", str(error), flush=True)
                    ch.basic_nack(method.delivery_tag, requeue=False)
                    return
                # Other failures exit the process and leave delivery unacknowledged.
                ch.basic_ack(method.delivery_tag)
                print(status, "redelivered=", method.redelivered, flush=True)

            channel.basic_consume(queue=QUEUE, on_message_callback=handle, auto_ack=False)
            print("consuming; Ctrl+C to stop", flush=True)
            try:
                channel.start_consuming()
            except KeyboardInterrupt:
                channel.stop_consuming()
    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    main()
