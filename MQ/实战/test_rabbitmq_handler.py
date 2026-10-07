"""Only the local business transaction is tested; this is not a broker test."""

import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from rabbitmq_lab import record_result


class HandlerTests(unittest.TestCase):
    def setUp(self):
        root = Path(os.environ.get("MQ_LAB_TMP", Path(__file__).parent / ".test-tmp"))
        root.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=root)
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "result.sqlite"
        self.message = {"schema_version": 1, "type": "ReportRequested",
                        "event_id": "e1", "task_id": "t1", "title": "report"}

    def test_redelivery_produces_one_result(self):
        self.assertEqual(record_result(self.path, self.message), "succeeded")
        self.assertEqual(record_result(self.path, self.message), "duplicate")
        db = sqlite3.connect(self.path)
        self.addCleanup(db.close)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM results").fetchone()[0], 1)

    def test_reused_id_with_changed_payload_is_rejected(self):
        record_result(self.path, self.message)
        with self.assertRaises(ValueError):
            record_result(self.path, dict(self.message, title="changed"))

    def test_new_event_id_does_not_duplicate_same_business_task(self):
        record_result(self.path, self.message)
        self.assertEqual(record_result(self.path, dict(self.message, event_id="e2")), "duplicate task")

    def test_invalid_and_poison_messages_have_no_effect(self):
        for message in ([], {}, dict(self.message, schema_version=2),
                        dict(self.message, task_id=None), dict(self.message, poison=True)):
            with self.assertRaises(ValueError):
                record_result(self.path, message)
        self.assertFalse(self.path.exists())


if __name__ == "__main__":
    unittest.main(verbosity=2)
