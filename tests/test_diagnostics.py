#!/usr/bin/env python3
"""End-to-end tests for the task-id blocking-diagnostics API.

Drives the same two-instance FastAPI deployment as selftest.py against a real
PostgreSQL and verifies:

  D1  input validation: malformed uuid -> 422; unknown uuid -> not_found
  D2  build lifecycle through diagnostics:
        not_started -> (blocked | scanning | waiting_unconfirmed) -> ready,
      no blocker is ever invented on a healthy build, log writes keep flowing
  D3  a real lock conflict: another session holding a write transaction pins
      the build in a wait phase; pg_blocking_pids() must expose its pid and
      diagnostics must report state=blocked; after COMMIT the build resumes
      and the blocker pid disappears
  D4  no guessing: terminated blocker while the build is still waiting -> the
      blocker pid vanishes from pg_blocking_pids and the state becomes
      waiting_unconfirmed (the old pid is not carried over)
  D5  generation replacement: after the old job row is replaced, diagnostics
      for the old task id return not_found (old phase/blocker never attached
      to the new generation); the new task id reports its own fresh state
  D6  backend exit / pid reuse guards: recorded pid gone -> backend_unconfirmed;
      backend_start mismatch (simulated pid reuse) -> backend_unconfirmed
  D7  terminal jobs never reuse a previous phase/blocker

Modes mirror selftest.py:
  local   spawns two local uvicorn processes
  docker  expects `docker compose` services api1/api2 to be running

Usage:
  python3 tests/test_diagnostics.py --mode local --dsn "<conninfo>"
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

import psycopg

INDEX_NAME = "idx_fault_logs_device_occurred"
GRACE = float(os.environ.get("ORPHAN_GRACE_SECONDS", "15"))
RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok)))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""),
          flush=True)
    if not ok:
        raise SystemExit(f"aborting: {name} failed {detail}")


def info(msg):
    print(f"  .. {msg}", flush=True)


def http(method, base, path, body=None, timeout=60):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            payload = json.loads(raw)
        except Exception:
            payload = {"raw": raw}
        return e.code, payload


def diag(base, task_id, timeout=30):
    return http("GET", base, f"/indexes/jobs/{task_id}/diagnostics", timeout=timeout)


class Db:
    def __init__(self, dsn):
        self.dsn = dsn

    def q(self, sql, params=None):
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            cur = conn.execute(sql, params or ())
            if cur.description is None:
                return []
            return cur.fetchall()

    def conn(self, autocommit=True):
        return psycopg.connect(self.dsn, autocommit=autocommit)


def wait_health(base, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            code, _ = http("GET", base, "/health", timeout=5)
            if code == 200:
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def wait_state(base, task_id, want, timeout=240):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        code, st = diag(base, task_id, timeout=30)
        last = st
        if code == 200 and st.get("state") == want:
            return st
        time.sleep(0.3)
    raise SystemExit(f"state never became {want!r}, last={last}")


def poll_diag(base, task_id, predicate, timeout=120):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        code, st = diag(base, task_id)
        last = st
        if code == 200 and predicate(st):
            return st
        time.sleep(0.25)
    raise SystemExit(f"predicate never matched, last={last}")


class LocalInstance:
    def __init__(self, name, port, dsn, workdir, logdir):
        self.name, self.port, self.dsn, self.workdir = name, port, dsn, workdir
        self.logpath = os.path.join(logdir, f"{name}-diag.log")
        self.proc = None

    def start(self):
        env = dict(os.environ)
        env["DATABASE_URL"] = self.dsn
        env["ORPHAN_GRACE_SECONDS"] = str(GRACE)
        self._log = open(self.logpath, "ab", buffering=0)
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(self.port)],
            cwd=self.workdir, env=env,
            stdout=self._log, stderr=subprocess.STDOUT)

    def kill(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()


def reset(db):
    db.q(f'DROP INDEX CONCURRENTLY IF EXISTS "{INDEX_NAME}"')
    db.q("DELETE FROM index_build_jobs")


def open_blocker_session(db):
    """Session holding RowExclusiveLock on the table pins CONCURRENTLY waits.

    Must use an explicit transaction: in autocommit mode the INSERT commits
    immediately and no lock is held.
    """
    conn = db.conn(autocommit=False)
    conn.execute("BEGIN")
    conn.execute(
        "INSERT INTO device_fault_logs (device_id, message) VALUES "
        "('diag-blocker', 'hold row lock for diagnostics test')")
    # keep the write transaction open (no commit)
    pid = conn.execute("SELECT pg_backend_pid()").fetchone()[0]
    return conn, pid


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

def phase_d1(ctx):
    print("\n== D1: input validation & unknown task id ==")
    base = ctx["bases"]["api1"]
    code, body = http("GET", base, "/indexes/jobs/not-a-uuid/diagnostics")
    check("D1 malformed uuid -> 422", code == 422, str(code))

    random_id = uuid.uuid4()
    code, st = diag(base, random_id)
    check("D1 unknown uuid -> 200 not_found",
          code == 200 and st["state"] == "not_found" and st["job"] is None
          and st["blocker_pids"] is None, str(st))

    # Swagger still serves the new endpoint.
    with urllib.request.urlopen(base + "/openapi.json", timeout=10) as r:
        spec = json.loads(r.read().decode())
    check("D1 endpoint registered in OpenAPI",
          "/indexes/jobs/{task_id}/diagnostics" in spec["paths"])


def phase_d2(ctx):
    print("\n== D2: lifecycle via diagnostics; no invented blockers; writes flow ==")
    db, base = ctx["db"], ctx["bases"]["api1"]
    reset(db)
    code, r = http("POST", base, "/indexes/build")
    check("D2 build started", code == 200 and r["state"] == "started", str(r))
    task_id = r["job_id"]

    # not_started may be missed because the worker registers quickly; accept it
    # if observed, otherwise require a live build state.
    observed = set()
    deadline = time.time() + 240
    writes_ok = 0
    final = None
    while time.time() < deadline:
        code, st = diag(base, task_id)
        assert code == 200, st
        observed.add(st["state"])
        check_state_invariants(st)
        if st["state"] in ("scanning", "blocked", "waiting_unconfirmed",
                           "initializing"):
            c, _ = http("POST", base, "/logs",
                        {"device_id": "diag-writer", "message": "write during build"})
            if c == 201:
                writes_ok += 1
        if st["state"] == "ready":
            final = st
            break
        time.sleep(0.05)
    check("D2 build reached ready via diagnostics", final is not None)
    check("D2 observed live build states",
          observed & {"scanning", "blocked", "waiting_unconfirmed",
                      "initializing", "not_started"}, str(observed))
    check("D2 writes kept flowing during the build", writes_ok > 0, str(writes_ok))
    check("D2 ready response carries no stale phase/blocker",
          final["phase"] is None and final["blocker_pids"] is None
          and final["backend"] is None, str({k: final[k] for k in
          ("phase", "blocker_pids", "backend")}))

    # A healthy build must never have reported a blocker.
    check("D2 no blocker invented on a healthy build",
          "blocked" not in observed, str(observed))
    # A healthy build that finishes between polls may be unconfirmable inside
    # the grace window, but must never be declared backend_unconfirmed.
    check("D2 healthy build never declared backend_unconfirmed",
          "backend_unconfirmed" not in observed, str(observed))

    # Query the job through the other instance too (stateless, shared DB).
    code, st = diag(ctx["bases"]["api2"], task_id)
    check("D2 other instance reports same ready job",
          code == 200 and st["state"] == "ready" and st["job"]["id"] == task_id)
    return task_id


def check_state_invariants(st):
    state = st["state"]
    if state == "blocked":
        assert st["blocker_pids"], f"blocked without blocker_pids: {st}"
        assert st["blockers"] and len(st["blockers"]) == len(st["blocker_pids"])
        assert st["phase"] and st["phase"]["is_wait_phase"], st["phase"]
        assert st["backend"] and st["backend"]["pid"]
    elif state in ("scanning", "initializing", "waiting_unconfirmed",
                   "unconfirmable"):
        # Never report guessed blockers.
        assert not st["blocker_pids"], f"blocker guessed in state {state}: {st}"
        if state in ("scanning", "initializing", "waiting_unconfirmed"):
            assert st["backend"] and st["backend"]["pid"]
        if state == "waiting_unconfirmed":
            assert st["phase"] and st["phase"]["is_wait_phase"]
    elif state in ("ready", "failed", "not_found", "not_started",
                   "backend_unconfirmed"):
        assert st["blocker_pids"] is None, f"stale blocker in {state}: {st}"


def phase_d3(ctx):
    print("\n== D3: real lock conflict -> blocked with confirmed pid -> resumes ==")
    db, base = ctx["db"], ctx["bases"]["api1"]
    reset(db)

    blocker, blocker_pid = open_blocker_session(db)
    info(f"blocker session pid={blocker_pid} holding open write transaction")

    code, r = http("POST", base, "/indexes/build")
    check("D3 build started under contention", r["state"] == "started", str(r))
    task_id = r["job_id"]

    st = poll_diag(
        base, task_id,
        lambda s: s["state"] in ("blocked", "waiting_unconfirmed",
                                 "scanning", "initializing"),
        timeout=90)
    # The build must get stuck in a wait phase behind the open transaction.
    st = poll_diag(base, task_id,
                   lambda s: s["phase"] is not None and s["phase"]["is_wait_phase"],
                   timeout=120)
    info(f"wait phase reached: {st['phase']['name']} "
         f"wait_event={st['backend']['wait_event_type']}/{st['backend']['wait_event']}")

    st = poll_diag(base, task_id, lambda s: s["state"] == "blocked", timeout=120)
    check("D3 state blocked", st["state"] == "blocked", st["state"])
    check("D3 blocker pid is the open transaction's pid",
          st["blocker_pids"] == [blocker_pid], str(st["blocker_pids"]))
    b0 = st["blockers"][0]
    check("D3 blocker details come from pg_stat_activity",
          b0["pid"] == blocker_pid and b0["backend_xid"] is not None
          and b0["query"] is not None, str(b0))
    check("D3 phase is a wait phase", st["phase"]["is_wait_phase"])
    # Database cross-check: pg_blocking_pids directly.
    rows = db.q("SELECT unnest(pg_blocking_pids(%s))",
                (st["backend"]["pid"],))
    check("D3 matches pg_blocking_pids() directly",
          [r[0] for r in rows] == [blocker_pid], str(rows))

    # Release the blocker; the build must resume and the pid must disappear.
    info("committing blocker transaction")
    blocker.commit()
    blocker.close()
    st2 = poll_diag(base, task_id,
                    lambda s: s["state"] in ("scanning", "ready",
                                             "waiting_unconfirmed",
                                             "initializing"),
                    timeout=60)
    check("D3 build resumed after COMMIT (no longer blocked)",
          st2["state"] != "blocked" and not st2["blocker_pids"],
          str(st2["state"]))
    final = wait_state(base, task_id, "ready", timeout=240)
    check("D3 build reached ready", final["job"]["id"] == task_id)
    check("D3 no blocker attached at ready",
          final["blocker_pids"] is None and final["phase"] is None)


def open_snapshot_session(db):
    """Read-only transaction holding an old snapshot (old backend_xmin).

    Pins CREATE INDEX CONCURRENTLY in the 'waiting for old snapshots' phase.
    """
    conn = db.conn(autocommit=False)
    conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
    conn.execute("SELECT count(*) FROM device_fault_logs "
                 "WHERE device_id = 'never-match-diag'")
    pid = conn.execute("SELECT pg_backend_pid()").fetchone()[0]
    return conn, pid


def phase_d4(ctx):
    print("\n== D4: blocker vanishes -> stale pid never carried over ==")
    db, base = ctx["db"], ctx["bases"]["api1"]
    reset(db)
    # A read-only transaction holding an old snapshot pins the build in the
    # 'waiting for old snapshots' phase and is confirmed by
    # pg_blocking_pids(). Terminating it must make that pid disappear from
    # every subsequent answer; the build then resumes and finishes. The
    # sub-millisecond waiting_unconfirmed window itself is covered
    # deterministically in test_diagnostics_unit.py (HTTP polling cannot
    # sample it reliably).
    blocker, blocker_pid = open_snapshot_session(db)
    info(f"snapshot-holder session pid={blocker_pid}")

    code, r = http("POST", base, "/indexes/build")
    task_id = r["job_id"]
    st = poll_diag(
        base, task_id,
        lambda s: s["state"] == "blocked"
        and s["phase"] and "old snapshots" in s["phase"]["raw_phase"],
        timeout=120)
    check("D4 build blocked in 'waiting for old snapshots' by confirmed pid",
          st["blocker_pids"] == [blocker_pid]
          and st["backend"]["wait_event"] == "virtualxid", str(st))

    db.q("SELECT pg_terminate_backend(%s)", (blocker_pid,))
    blocker.close()
    info("snapshot holder terminated; the dead pid must never be reported again")

    # From now on no response may ever name the terminated pid as a blocker;
    # if a wait phase is still visible it must be waiting_unconfirmed (empty
    # blocker list), never the stale pid.
    deadline = time.time() + 240
    final = None
    saw_unconfirmed = False
    while time.time() < deadline:
        code, st = diag(base, task_id)
        assert blocker_pid not in (st.get("blocker_pids") or []), st
        if st["state"] == "waiting_unconfirmed":
            saw_unconfirmed = True
            check("D4 waiting_unconfirmed carries no guessed blocker",
                  st["blocker_pids"] == [] and st["blockers"] is None
                  and st["phase"]["is_wait_phase"], str(st))
        if st["state"] == "ready":
            final = st
            break
        time.sleep(0.03)
    check("D4 build eventually ready after blocker loss", final is not None)
    check("D4 no phase/blocker attached at ready",
          final["phase"] is None and final["blocker_pids"] is None)
    print(f"  .. waiting_unconfirmed sampled over HTTP: {saw_unconfirmed} "
          "(deterministic coverage in test_diagnostics_unit.py)")


def phase_d5(ctx):
    print("\n== D5: job generation replacement isolates old diagnostics ==")
    db = ctx["db"]
    b1, b2 = ctx["bases"]["api1"], ctx["bases"]["api2"]
    reset(db)

    # Generation 1: start a build, then interrupt it like the P4b crash path.
    code, r = http("POST", b1, "/indexes/build")
    check("D5 gen1 started", r["state"] == "started", str(r))
    old_id = r["job_id"]
    deadline = time.time() + 60
    while time.time() < deadline:
        cat = db.q(
            """SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
               WHERE c.relname=%s AND n.nspname='public' AND c.relkind='i'""",
            (INDEX_NAME,))
        if cat:
            break
        time.sleep(0.2)
    rows = db.q(
        "SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE pid <> pg_backend_pid() AND query ~* '^\\s*create\\s+index' "
        "AND query LIKE %s", (f"%{INDEX_NAME}%",))
    check("D5 gen1 build backend terminated", len(rows) >= 1, str(rows))

    # Diagnostics while the ledger row still exists but the backend is gone.
    # Inside the grace window that is honestly unconfirmable; after the grace
    # window the same observation becomes backend_unconfirmed.
    st = poll_diag(b2, old_id,
                   lambda s: s["state"] in ("backend_unconfirmed",
                                            "unconfirmable"),
                   timeout=60)
    check("D5 exited backend inside grace -> unconfirmable (no guess)",
          st["state"] == "unconfirmable" and st["phase"] is None
          and st["blocker_pids"] is None and st["backend"] is None
          and st["job"]["id"] == old_id, str(st))

    info(f"waiting {GRACE + 3:.0f}s for orphan grace")
    time.sleep(GRACE + 3)
    code, st = http("GET", b2, "/indexes/status")
    check("D5 gen1 orphan marked failed", st["state"] == "failed", str(st))
    code, st = diag(b2, old_id)
    check("D5 after grace, exited backend would be backend_unconfirmed; "
          "job now failed with no phase/blocker",
          st["state"] == "failed" and st["phase"] is None
          and st["blocker_pids"] is None)

    # Cleanup + retry -> the old row is deleted and a new generation inserted.
    code, r = http("POST", b2, "/indexes/cleanup")
    check("D5 cleanup done", r.get("cleaned"), str(r))
    code, r = http("POST", b2, "/indexes/build")
    check("D5 gen2 started", r["state"] == "started", str(r))
    new_id = r["job_id"]
    check("D5 new generation has a different task id", new_id != old_id)

    code, st = diag(b1, old_id)
    check("D5 old task id -> not_found (nothing carried to the new generation)",
          st["state"] == "not_found" and st["job"] is None
          and st["phase"] is None and st["blocker_pids"] is None, str(st))

    st = wait_state(b2, new_id, "ready", timeout=240)
    check("D5 new generation reports its own ready job",
          st["job"]["id"] == new_id and st["state"] == "ready")
    code, st = diag(b2, new_id)
    check("D5 new generation diagnostics never mention the old id",
          st["job"]["id"] != old_id)


def phase_d6(ctx):
    print("\n== D6: backend exit / pid-reuse guards ==")
    db, base = ctx["db"], ctx["bases"]["api1"]
    reset(db)
    code, r = http("POST", base, "/indexes/build")
    task_id = r["job_id"]
    st = wait_state(base, task_id, "ready", timeout=240)
    check("D6 build ready", st["state"] == "ready")

    rows = db.q("SELECT backend_pid, backend_start FROM index_build_jobs "
                "WHERE id = %s", (uuid.UUID(task_id),))
    check("D6 ledger recorded backend identity",
          rows and rows[0][0] is not None and rows[0][1] is not None, str(rows))
    recorded_pid, recorded_start = rows[0]

    # A live, idle session stands in for a backend that reused a pid.
    holder = db.conn()
    holder_pid, holder_start = holder.execute(
        "SELECT pid, backend_start FROM pg_stat_activity "
        "WHERE pid = pg_backend_pid()").fetchone()

    # 6a: pid exists but backend_start disagrees -> pid reuse.
    fake_start = holder_start.replace(year=holder_start.year - 1)
    db.q("UPDATE index_build_jobs SET status='building', backend_pid=%s, "
         "backend_start=%s, created_at=now() - interval '60 seconds' "
         "WHERE id=%s",
         (holder_pid, fake_start, uuid.UUID(task_id)))
    code, st = diag(base, task_id)
    check("D6a backend_start mismatch -> backend_unconfirmed (pid reuse guard)",
          st["state"] == "backend_unconfirmed"
          and "reused" in st["reason"] and st["blocker_pids"] is None, str(st))

    # 6b: pid and start match, but that backend is not running our DDL.
    db.q("UPDATE index_build_jobs SET backend_start=%s WHERE id=%s",
         (holder_start, uuid.UUID(task_id)))
    code, st = diag(base, task_id)
    check("D6b wrong statement at pid -> backend_unconfirmed",
          st["state"] == "backend_unconfirmed"
          and "not actively executing" in st["reason"], str(st))
    holder.close()

    # 6c: recorded pid vanished from pg_stat_activity (backend exited).
    db.q("UPDATE index_build_jobs SET backend_pid=%s, backend_start=%s "
         "WHERE id=%s", (2_000_000, fake_start, uuid.UUID(task_id)))
    code, st = diag(base, task_id)
    check("D6c vanished pid -> backend_unconfirmed",
          st["state"] == "backend_unconfirmed" and st["backend"] is None
          and "absent" in st["reason"], str(st))

    # Restore the row to its real terminal state.
    db.q("UPDATE index_build_jobs SET status='ready', error=NULL, "
         "backend_pid=%s, backend_start=%s WHERE id=%s",
         (recorded_pid, recorded_start, uuid.UUID(task_id)))
    code, st = diag(base, task_id)
    check("D6 restored row reports ready again", st["state"] == "ready")


def phase_d7(ctx):
    print("\n== D7: terminal state never reuses old conclusions ==")
    db, base = ctx["db"], ctx["bases"]["api1"]
    # Reuse the ready job from D6; mark it failed at the ledger and ensure no
    # phase/blocker fields leak.
    rows = db.q("SELECT id FROM index_build_jobs LIMIT 1")
    task_id = str(rows[0][0])
    db.q("UPDATE index_build_jobs SET status='failed', error='injected' "
         "WHERE id=%s", (uuid.UUID(task_id),))
    code, st = diag(base, task_id)
    check("D7 failed job: terminal, no phase, no blocker, error surfaced",
          st["state"] == "failed" and st["phase"] is None
          and st["blocker_pids"] is None and st["job"]["error"] == "injected",
          str(st))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["local", "docker"], required=True)
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--api1", default="http://127.0.0.1:8001")
    ap.add_argument("--api2", default="http://127.0.0.1:8002")
    ap.add_argument("--workdir", default="/workspace")
    ap.add_argument("--logdir", default="/tmp/ops-logs")
    args = ap.parse_args()

    os.makedirs(args.logdir, exist_ok=True)
    db = Db(args.dsn)
    instances = {}
    if args.mode == "local":
        subprocess.run(["pkill", "-f", "uvicorn app.main:app"],
                       capture_output=True)
        time.sleep(1)
        instances = {
            "api1": LocalInstance("api1", 8001, args.dsn, args.workdir, args.logdir),
            "api2": LocalInstance("api2", 8002, args.dsn, args.workdir, args.logdir),
        }
        for inst in instances.values():
            inst.start()

    ctx = {"mode": args.mode, "db": db, "instances": instances,
           "bases": {"api1": args.api1, "api2": args.api2}}
    try:
        for name, base in ctx["bases"].items():
            check(f"health {name}", wait_health(base), base)
        phase_d1(ctx)
        phase_d2(ctx)
        phase_d3(ctx)
        phase_d4(ctx)
        phase_d5(ctx)
        phase_d6(ctx)
        phase_d7(ctx)
    finally:
        passed = sum(1 for _, ok in RESULTS if ok)
        print(f"\n==== diagnostics summary: {passed}/{len(RESULTS)} checks passed ====")
        if args.mode == "local":
            for inst in instances.values():
                inst.kill()
    if not all(ok for _, ok in RESULTS):
        sys.exit(1)
    print("ALL DIAGNOSTIC CHECKS PASSED")


if __name__ == "__main__":
    main()
