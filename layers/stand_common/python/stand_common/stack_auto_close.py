"""STACK deadline data and durable outbox entries."""
import math
import os
import time
import uuid


GRACE_SECONDS = 120
MAX_TIMESTAMP = 253402300799  # UTC, end of year 9999


def scheduled_at(meta):
    schedule = meta.get("stackAutoClose")
    return int(schedule["closesAt"]) if schedule else None


def starts_closed(meta):
    deadline = scheduled_at(meta)
    return deadline is not None and time.time() >= deadline


def finish_deadline(meta):
    if "stackFinishDeadline" in meta:
        return int(meta["stackFinishDeadline"])
    deadline = scheduled_at(meta)
    return deadline + GRACE_SECONDS if deadline is not None else None


def configure(previous, duration):
    if duration is None:
        return None
    if not isinstance(duration, dict) or set(duration) != {"hours", "minutes"}:
        raise ValueError("InvalidStackAutoClose")
    hours, minutes = duration["hours"], duration["minutes"]
    if (type(hours) is not int or type(minutes) is not int
            or hours < 0 or not 0 <= minutes < 60 or not (hours or minutes)):
        raise ValueError("InvalidStackAutoClose")
    closes_at = math.ceil(time.time()) + hours * 3600 + minutes * 60
    if closes_at + GRACE_SECONDS > MAX_TIMESTAMP:
        raise ValueError("InvalidStackAutoClose")
    if previous and previous["hours"] == hours and previous["minutes"] == minutes:
        return previous
    return {**duration, "id": "auto_close_" + uuid.uuid4().hex, "closesAt": closes_at}


def view(schedule):
    return {key: schedule[key] for key in ("hours", "minutes", "closesAt")} if schedule else None


def task(game_id, schedule):
    return {"gameId": game_id, "id": schedule["id"], "kind": "auto_close",
            "work": "pending", "dueAt": schedule["closesAt"]}


def delete_task(game_id, schedule):
    return {"Delete": {"TableName": os.environ["STACK_PRIZES_TABLE"],
                       "Key": {"gameId": game_id, "id": schedule["id"]}}}
