"""Blocking diagnostics for a single online index-build job.

The diagnosis is computed entirely from *live* PostgreSQL views on every
request; nothing is cached, so an old phase or an old blocker can never be
re-attributed to a newer job generation:

* the ledger row is looked up by task id -- a replaced/deleted row simply
  does not exist anymore -> 'not_found'
* the executing backend is identified by (backend_pid, backend_start) as
  recorded by the build worker -- a reused pid fails the backend_start check
  -> 'backend_unconfirmed'
* the build phase comes from pg_stat_progress_create_index
* blocker pids come from pg_blocking_pids(), i.e. the database's own answer.
  If it returns no pid while the backend is visibly waiting, the state is
  'waiting_unconfirmed': we report the wait event but never invent a blocker.
"""

import logging
import re
import uuid
from typing import Optional

from . import index_manager as im
from .db import connect

log = logging.getLogger("diagnostics")

# pg_stat_progress_create_index.phase is a text label (the internal integer
# codes are not exposed). Labels differ across major versions, so match on
# stable substrings. Wait phases are the ones in which the build can block.
WAIT_PHASE_MARKERS = (
    "waiting for writers",
    "waiting for old snapshots",
    "waiting for readers",
)


def _classify_phase(raw_phase: str) -> dict:
    text = raw_phase or ""
    low = text.lower()
    is_wait = any(marker in low for marker in WAIT_PHASE_MARKERS)
    if low.startswith("initializing"):
        canonical = "initializing"
    elif is_wait:
        canonical = text
    elif "validation" in low:
        canonical = "index validation"
    elif "building index" in low:
        canonical = "building index"
    else:
        canonical = "unknown phase"
    return {"name": canonical, "is_wait_phase": is_wait, "raw_phase": text}

_DDL_PATTERN = r"^\s*create\s+index\b"


def _iso(value):
    return value.isoformat() if value is not None else None


def _job_dict(row) -> dict:
    return {
        "id": str(row[0]),
        "status": row[1],
        "builder": row[2],
        "error": row[3],
        "created_at": _iso(row[4]),
        "updated_at": _iso(row[5]),
        "backend_pid": row[6],
        "backend_start": _iso(row[7]),
        "index_name": row[8],
        "age_s": float(row[9]),
    }


def _phase_dict(progress: Optional[tuple]) -> Optional[dict]:
    if not progress:
        return None
    raw_phase = progress[0]
    classified = _classify_phase(raw_phase)
    return {
        "name": classified["name"],
        "raw_phase": classified["raw_phase"],
        "is_wait_phase": classified["is_wait_phase"],
        "metrics": {
            "lockers_total": progress[1],
            "lockers_done": progress[2],
            "current_locker_pid": progress[3],
            "blocks_total": progress[4],
            "blocks_done": progress[5],
            "tuples_total": progress[6],
            "tuples_done": progress[7],
            "partitions_total": progress[8],
            "partitions_done": progress[9],
        },
    }


def _backend_dict(activity: Optional[tuple]) -> Optional[dict]:
    if not activity:
        return None
    return {
        "pid": activity[0],
        "backend_start": _iso(activity[1]),
        "state": activity[2],
        "wait_event_type": activity[3],
        "wait_event": activity[4],
        "query_start": _iso(activity[5]),
        "state_change": _iso(activity[6]),
    }


def _blocker_dict(row) -> dict:
    return {
        "pid": row[0],
        "usename": row[1],
        "application_name": row[2],
        "state": row[3],
        "wait_event_type": row[4],
        "wait_event": row[5],
        "backend_xid": row[6],
        "backend_xmin": row[7],
        "query_start": _iso(row[8]),
        "state_change": _iso(row[9]),
        "query": row[10],
    }


def diagnose_job(task_id: uuid.UUID) -> dict:
    """Return live blocking diagnostics for one index-build task.

    States:
      not_found            no ledger row with this id (never existed or a
                           newer job generation replaced it)
      ready / failed       terminal job: no phase/blocker is carried over
      not_started          ledger says building but no backend registered yet
      backend_unconfirmed  recorded backend exited, pid was reused, or the
                           pid is not executing this job's CREATE INDEX
                           (only after the orphan grace window; inside it the
                           same observation is reported as unconfirmable,
                           since the build may simply be starting/finishing)
      blocked              pg_blocking_pids() names at least one blocker pid
      scanning             build/validation phase progressing, no blocker
      initializing         DDL active, still in the initializing phase
      waiting_unconfirmed  backend is in a wait phase and pg_blocking_pids()
                           returns no pid -- blocker cannot be confirmed
      unconfirmable        DDL is active but the progress view has no row, or
                           the backend identity cannot yet be checked within
                           the grace window
    """
    with connect(autocommit=True) as conn:
        row = conn.execute(
            """
            SELECT id, status, builder, error, created_at, updated_at,
                   backend_pid, backend_start, index_name,
                   EXTRACT(EPOCH FROM (now() - created_at))::float AS age_s
            FROM index_build_jobs
            WHERE id = %s
            """,
            (task_id,),
        ).fetchone()

        cat = im._catalog_state(conn)

        if row is None:
            # A replaced generation's row is gone: there is nothing to carry
            # old phases or blockers over from.
            return _envelope(task_id, "not_found", None, None, None, None, None,
                             cat, reason="no index_build_jobs row for this task id")

        job = _job_dict(row)

        if job["status"] in ("ready", "failed"):
            # Terminal jobs must never reuse a previously observed phase/blocker.
            return _envelope(task_id, job["status"], job, None, None, None, None,
                             cat,
                             reason="terminal job state; no live build to diagnose")

        # status == 'building' from here on.
        if job["backend_pid"] is None:
            return _envelope(task_id, "not_started", job, None, None, None, None,
                             cat,
                             reason="job claimed but the build worker has not "
                                    "registered its backend yet")

        activity = conn.execute(
            """
            SELECT pid, backend_start, state, wait_event_type, wait_event,
                   query_start, state_change, query
            FROM pg_stat_activity
            WHERE pid = %s
            """,
            (job["backend_pid"],),
        ).fetchone()

        # Within the orphan grace window the build may be between claim and
        # DDL start, or just finishing (connection closed, _finish pending).
        # Do not assert the backend is gone in that window; report the
        # observation as unconfirmable and never attach a phase or blocker.
        in_grace = job["age_s"] < im.GRACE_SECONDS

        if activity is None:
            reason = ("recorded backend pid is absent from pg_stat_activity "
                      "(backend exited; pid may be reused)")
            if in_grace:
                return _unconfirmable(task_id, job, cat, reason +
                                      "; inside grace window, not conclusive")
            return _unconfirmed(task_id, job, cat, reason)

        pid, backend_start, state, wait_type, wait_event = activity[0:5]
        query = activity[7]

        if backend_start != row[7]:
            # PID reuse guard: the pid now belongs to a different backend.
            reason = ("pid reused: backend_start in pg_stat_activity does not "
                      "match the value recorded for this job")
            if in_grace:
                return _unconfirmable(task_id, job, cat, reason +
                                      "; inside grace window, not conclusive")
            return _unconfirmed(task_id, job, cat, reason)

        is_ddl = (
            state == "active"
            and query is not None
            and _matches_ddl(query, job["index_name"])
        )
        if not is_ddl:
            reason = ("the recorded pid is not actively executing this job's "
                      "CREATE INDEX statement")
            if in_grace:
                return _unconfirmable(task_id, job, cat, reason +
                                      "; inside grace window, not conclusive")
            return _unconfirmed(task_id, job, cat, reason)

        progress = conn.execute(
            """
            SELECT phase,
                   lockers_total, lockers_done, current_locker_pid,
                   blocks_total, blocks_done,
                   tuples_total, tuples_done,
                   partitions_total, partitions_done
            FROM pg_stat_progress_create_index
            WHERE pid = %s
            """,
            (pid,),
        ).fetchone()
        phase = _phase_dict(progress)

        # The database's own answer: only pids returned here are ever reported
        # as blockers.
        blocker_pids = [
            r[0]
            for r in conn.execute(
                "SELECT pid FROM unnest(pg_blocking_pids(%s)) AS pid", (pid,)
            ).fetchall()
        ]

        backend = _backend_dict(activity)

        if blocker_pids:
            details = conn.execute(
                """
                SELECT pid, usename, application_name, state, wait_event_type,
                       wait_event, backend_xid::text, backend_xmin::text,
                       query_start, state_change, query
                FROM pg_stat_activity
                WHERE pid = ANY(%s)
                ORDER BY pid
                """,
                (blocker_pids,),
            ).fetchall()
            blockers = [_blocker_dict(r) for r in details]
            return _envelope(
                task_id, "blocked", job, backend, phase, blocker_pids, blockers,
                cat,
                reason="pg_blocking_pids() confirms sessions blocking the "
                       "index-build backend")

        # No blocker confirmed by the database.
        if phase is None:
            return _envelope(
                task_id, "unconfirmable", job, backend, None, None, None, cat,
                reason="CREATE INDEX is active but no "
                       "pg_stat_progress_create_index row is visible yet; "
                       "phase and blocker cannot be confirmed")

        if phase["is_wait_phase"]:
            return _envelope(
                task_id, "waiting_unconfirmed", job, backend, phase, [], None,
                cat,
                reason=(f"backend is in wait phase {phase['name']!r} "
                        f"(wait_event_type={wait_type}, wait_event={wait_event}) "
                        "but pg_blocking_pids() returns no pid; blocker cannot "
                        "be confirmed and is not guessed"))

        if phase["name"] == "initializing":
            diag_state = "initializing"
            reason = "DDL active in the initializing phase; no blocker reported"
        else:
            diag_state = "scanning"
            reason = "build/validation phase in progress; no blocker reported"
        return _envelope(task_id, diag_state, job, backend, phase, None, None,
                         cat, reason=reason)


def _matches_ddl(query_text: str, index_name: str) -> bool:
    return (
        re.match(_DDL_PATTERN, query_text, flags=re.IGNORECASE) is not None
        and index_name.lower() in query_text.lower()
    )


def _unconfirmed(task_id, job, cat, reason: str) -> dict:
    log.info("diagnostics for job %s unconfirmed: %s", job["id"], reason)
    return _envelope(task_id, "backend_unconfirmed", job, None, None, None, None,
                     cat, reason=reason)


def _unconfirmable(task_id, job, cat, reason: str) -> dict:
    log.info("diagnostics for job %s unconfirmable: %s", job["id"], reason)
    return _envelope(task_id, "unconfirmable", job, None, None, None, None,
                     cat, reason=reason)


def _envelope(task_id, state, job, backend, phase, blocker_pids, blockers,
              cat, reason="") -> dict:
    return {
        "task_id": str(task_id),
        "state": state,
        "reason": reason,
        "index_name": im.INDEX_NAME,
        "job": job,
        "backend": backend,
        "phase": phase,
        "blocker_pids": blocker_pids,
        "blockers": blockers,
        "catalog": cat,
        "handled_by": im.INSTANCE_ID,
    }
