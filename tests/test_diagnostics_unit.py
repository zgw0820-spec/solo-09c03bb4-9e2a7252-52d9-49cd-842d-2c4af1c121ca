#!/usr/bin/env python3
"""Deterministic unit tests for diagnostics state classification.

The live 'waiting_unconfirmed' window inside PostgreSQL's lock/snapshot
waiting machinery is sub-millisecond (the build advances to the next locker
in <10ms), so it cannot be sampled reliably through HTTP polling. These
tests stub the database-facing calls to exercise every classification branch
deterministically, including the never-guess-a-blocker logic.
"""

import datetime
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import diagnostics  # noqa: E402

TASK_ID = uuid.uuid4()
CAT = {"exists": True, "valid": False, "ready": False, "definition": None}
BACKEND_START = datetime.datetime(2026, 11, 18, tzinfo=datetime.timezone.utc)
OTHER_START = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)


class FakeCursor:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows


class FakeConn:
    """Returns scripted result sets in the order diagnose_job issues queries.

    The catalog inspection (_catalog_state) is short-circuited to a fixed
    value so scripts only need to cover the job/activity/progress/blocker
    queries, in that order.
    """

    def __init__(self, scripts):
        self.scripts = list(scripts)

    def execute(self, sql, params=None):
        if "pg_class" in sql:  # catalog-state query
            return FakeCursor([(False, False, None)])
        rows = self.scripts.pop(0)
        return FakeCursor(rows)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def job_row(status="building", pid=100, start=BACKEND_START, age=99.0):
    # id, status, builder, error, created_at, updated_at, backend_pid,
    # backend_start, index_name, age_s
    return (TASK_ID, status, "inst", None, None, None, pid, start,
            diagnostics.im.INDEX_NAME, age)


def activity_row(pid=100, start=BACKEND_START, state="active",
                 wait_type="Lock", wait_event="virtualxid",
                 query=None):
    return (pid, start, state, wait_type, wait_event, None, None,
            query or f'CREATE INDEX CONCURRENTLY "{diagnostics.im.INDEX_NAME}" '
                     f"ON {diagnostics.im.TABLE_NAME} (device_id, occurred_at)")


def progress_row(phase):
    # phase + 9 metric columns
    return (phase, 1, 0, 0, 0, 0, 0, 0, 0, 0)


RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok)))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))
    assert ok, f"{name}: {detail}"


def run(scripts):
    import contextlib
    orig = diagnostics.connect
    diagnostics.connect = lambda autocommit=False: FakeConn(scripts)
    try:
        return diagnostics.diagnose_job(TASK_ID)
    finally:
        diagnostics.connect = orig


def test_not_found():
    st = run([[]])  # no ledger row -> catalog query still runs
    check("not_found", st["state"] == "not_found" and st["job"] is None
          and st["blocker_pids"] is None)


def test_terminal_ready_and_failed():
    st = run([[job_row(status="ready")]])
    check("ready: no phase/blocker carried",
          st["state"] == "ready" and st["phase"] is None
          and st["blocker_pids"] is None and st["backend"] is None)
    st = run([[job_row(status="failed", pid=None)]])
    check("failed: no phase/blocker carried, error present",
          st["state"] == "failed" and st["phase"] is None
          and st["blocker_pids"] is None)


def test_not_started():
    st = run([[job_row(pid=None)]])
    check("not_started when no backend registered",
          st["state"] == "not_started" and st["blocker_pids"] is None)


def test_pid_reuse():
    # activity exists but backend_start disagrees, outside grace.
    st = run([[job_row(age=99)],
              [activity_row(start=OTHER_START)]])
    check("pid reuse -> backend_unconfirmed",
          st["state"] == "backend_unconfirmed" and "reused" in st["reason"]
          and st["blocker_pids"] is None)


def test_pid_gone_within_grace():
    st = run([[job_row(age=1.0)], []])
    check("vanished pid inside grace -> unconfirmable, not a conclusion",
          st["state"] == "unconfirmable" and st["blocker_pids"] is None
          and st["backend"] is None)


def test_pid_gone_after_grace():
    st = run([[job_row(age=99)], []])
    check("vanished pid after grace -> backend_unconfirmed",
          st["state"] == "backend_unconfirmed" and "absent" in st["reason"])


def test_wrong_statement():
    st = run([[job_row(age=99)],
              [activity_row(query="SELECT 1")]])
    check("pid not running our DDL -> backend_unconfirmed",
          st["state"] == "backend_unconfirmed"
          and "not actively executing" in st["reason"])


def test_blocked():
    # job, activity, progress, pg_blocking_pids, blocker details
    st = run([[job_row()],
              [activity_row()],
              [progress_row("waiting for writers before build")],
              [(200,)],
              [(200, "ops", "", "idle in transaction", "Client", "ClientRead",
                "555", None, None, None, "INSERT ...")]])
    check("blocked with confirmed pid",
          st["state"] == "blocked" and st["blocker_pids"] == [200]
          and st["blockers"][0]["pid"] == 200
          and st["phase"]["is_wait_phase"])


def test_waiting_unconfirmed_no_guess():
    # wait phase, but pg_blocking_pids returns no pid -> must not invent one.
    st = run([[job_row()],
              [activity_row(wait_type="Lock", wait_event="virtualxid")],
              [progress_row("waiting for old snapshots")],
              []])  # empty pg_blocking_pids
    check("wait phase without confirmed pid -> waiting_unconfirmed",
          st["state"] == "waiting_unconfirmed"
          and st["blocker_pids"] == [] and st["blockers"] is None
          and st["phase"]["is_wait_phase"])
    check("no blocker invented", st["blocker_pids"] == [])


def test_scanning():
    st = run([[job_row()],
              [activity_row(wait_type="IO", wait_event="DataFileRead")],
              [progress_row("building index: scanning table")],
              []])
    check("scanning phase, no blocker",
          st["state"] == "scanning" and st["blocker_pids"] is None
          and not st["phase"]["is_wait_phase"])


def test_initializing():
    st = run([[job_row()],
              [activity_row(wait_type=None, wait_event=None)],
              [progress_row("initializing")],
              []])
    check("initializing phase",
          st["state"] == "initializing" and st["blocker_pids"] is None)


def test_ddl_active_without_progress():
    st = run([[job_row()],
              [activity_row()],
              [],  # no progress row
              []])
    check("active DDL without progress row -> unconfirmable",
          st["state"] == "unconfirmable" and st["phase"] is None
          and st["blocker_pids"] is None)


def main():
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print(f"\n==== diagnostics unit summary: {len(RESULTS)}/{len(RESULTS)} "
          "checks passed ====")


if __name__ == "__main__":
    main()
