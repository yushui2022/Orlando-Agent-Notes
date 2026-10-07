"""Tests cross-process crashes, not just successful enqueue/dequeue."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from reliable_queue import Lab


class ReliabilityTests(unittest.TestCase):
    def setUp(self):
        root = Path(os.environ.get("MQ_LAB_TMP", Path(__file__).parent / ".test-tmp"))
        root.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=root)
        self.addCleanup(self.temp.cleanup)
        self.lab = Lab(self.temp.name)

    def child(self, *args):
        return subprocess.run(
            [sys.executable, str(Path(__file__).with_name("reliable_queue.py")),
             "--data", self.temp.name, "--now", "0", *args],
            capture_output=True, text=True, timeout=15)

    def ready(self):
        self.lab.submit("t1", "hello")
        self.lab.relay()

    def assert_once(self):
        status = self.lab.status()
        self.assertEqual(len(status["results"]), 1)
        self.assertEqual(len(status["inbox"]), 1)
        self.assertEqual(status["tasks"][0]["state"], "succeeded")

    def test_submit_is_idempotent_and_rejects_changed_payload(self):
        event = self.lab.submit("t1", "hello")
        self.assertEqual(self.lab.submit("t1", "hello"), event)
        with self.assertRaises(ValueError):
            self.lab.submit("t1", "changed")
        self.assertEqual(len(self.lab.status()["outbox"]), 1)

    def test_outbox_survives_application_restart(self):
        self.lab.submit("t1", "hello")
        self.assertEqual(self.lab.status()["deliveries"], [])
        restarted = Lab(self.temp.name)
        self.assertEqual(restarted.relay(), 1)
        self.assertEqual(restarted.work(0), "succeeded")
        self.assert_once()

    def test_publisher_crash_causes_duplicate_not_missing_work(self):
        self.lab.submit("t1", "hello")
        result = self.child("relay", "--crash", "after-publish")
        self.assertEqual(result.returncode, 86, result.stderr)
        self.assertEqual(self.lab.status()["outbox"][0]["sent"], 0)
        self.lab.relay()
        self.assertEqual(len(self.lab.status()["deliveries"]), 2)
        self.assertEqual(self.lab.work(0), "succeeded")
        self.assertEqual(self.lab.work(0), "duplicate")
        self.assert_once()

    def test_claim_crash_is_recovered_only_after_lease(self):
        self.ready()
        result = self.child("work", "--crash", "after-claim")
        self.assertEqual(result.returncode, 86, result.stderr)
        self.assertEqual(self.lab.work(4), "empty")
        self.assertEqual(self.lab.work(5), "succeeded")
        self.assert_once()

    def test_process_death_rolls_back_inbox_and_result_together(self):
        self.ready()
        result = self.child("work", "--crash", "before-commit")
        self.assertEqual(result.returncode, 86, result.stderr)
        status = self.lab.status()
        self.assertEqual(status["inbox"], [])
        self.assertEqual(status["results"], [])
        self.assertEqual(status["tasks"][0]["state"], "queued")
        self.assertEqual(self.lab.work(5), "succeeded")
        self.assert_once()

    def test_commit_then_crash_redelivers_without_second_effect(self):
        self.ready()
        result = self.child("work", "--crash", "after-commit")
        self.assertEqual(result.returncode, 86, result.stderr)
        self.assert_once()
        self.assertEqual(self.lab.status()["deliveries"][0]["state"], "inflight")
        self.assertEqual(self.lab.work(5), "duplicate")
        self.assertEqual(self.lab.status()["deliveries"][0]["state"], "acked")
        self.assert_once()

    def test_poison_message_has_backoff_and_bounded_attempts(self):
        self.ready()
        self.assertEqual(self.lab.work(0, simulate_failure=True), "retry")
        self.assertEqual(self.lab.work(0.5), "empty")
        self.assertEqual(self.lab.work(1, simulate_failure=True), "retry")
        self.assertEqual(self.lab.work(2), "empty")
        self.assertEqual(self.lab.work(3, simulate_failure=True), "dead")
        self.assertEqual(self.lab.work(100), "empty")
        self.assertEqual(self.lab.status()["tasks"][0]["state"], "failed")
        self.assertEqual(self.lab.status()["results"], [])

    def test_transient_failure_can_succeed_later(self):
        self.ready()
        self.assertEqual(self.lab.work(0, simulate_failure=True), "retry")
        self.assertEqual(self.lab.work(1), "succeeded")
        self.assert_once()

    def test_old_receipt_cannot_ack_new_delivery(self):
        self.ready()
        old = self.lab.claim(0)
        new = self.lab.claim(5)
        self.assertNotEqual(old["token"], new["token"])
        self.assertFalse(self.lab.ack(old))
        self.assertTrue(self.lab.ack(new))


if __name__ == "__main__":
    unittest.main(verbosity=2)
