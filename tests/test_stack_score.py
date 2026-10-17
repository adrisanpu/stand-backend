"""Offline STACK attempt lifecycle, including concurrent conditional writes."""
import importlib.util
import sys
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from threading import Barrier, Lock
from unittest.mock import MagicMock

import pytest

from stand_common.stack import DEFAULT_CONFIG, public_assignment, replay, get_ranking

SCORE_DIR = Path(__file__).parent.parent / "src" / "stand_prod_game_score"
sys.path.insert(0, str(SCORE_DIR))
spec = importlib.util.spec_from_file_location("stack_score", SCORE_DIR / "games" / "stack.py")
stack = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stack)

META = {
    "gameId": "ABCDEF", "gameType": "STACK", "isActive": True,
    "type": {"STACK": deepcopy(DEFAULT_CONFIG)},
}


class ConditionalFailure(Exception):
    response = {"Error": {"Code": "ConditionalCheckFailedException"}}


class AttemptTable:
    """Model the exact DynamoDB condition under a lock; no AWS calls."""

    def __init__(self, concurrent=False):
        self.player = {"gameId": "ABCDEF", "playerId": 7, "type": {"STACK": {}}}
        self.lock = Lock()
        self.barrier = Barrier(2) if concurrent else None
        self.reads = 0
        self.writes = 0

    def get_item(self, *, Key, ConsistentRead):
        assert Key == {"gameId": "ABCDEF", "playerId": 7}
        assert ConsistentRead is True
        with self.lock:
            snapshot = deepcopy(self.player)
            self.reads += 1
            wait = self.barrier and self.reads <= 2
        if wait:
            self.barrier.wait(timeout=5)
        return {"Item": snapshot} if snapshot else {}

    def update_item(self, **kwargs):
        values = kwargs["ExpressionAttributeValues"]
        finishing = ":score" in values
        if finishing:
            assert kwargs["UpdateExpression"] == (
                "SET #type.#g.#attempt = :attempt, #type.#g.#score = :score, #type.#g.#updated = :now"
            )
            assert kwargs["ConditionExpression"] == (
                "attribute_exists(gameId) AND attribute_exists(playerId) "
                "AND #type.#g.#attempt.#id = :id AND #type.#g.#attempt.#status = :started "
                "AND attribute_not_exists(#type.#g.#score)"
            )
        else:
            assert kwargs["UpdateExpression"] == "SET #type.#g.#attempt = :attempt"
            assert kwargs["ConditionExpression"] == (
                "attribute_exists(gameId) AND attribute_exists(playerId) "
                "AND attribute_not_exists(#type.#g.#attempt) "
                "AND attribute_not_exists(#type.#g.#score)"
            )
        with self.lock:
            if not self.player:
                raise ConditionalFailure()
            blob = self.player["type"]["STACK"]
            attempt = blob.get("attempt") or {}
            if "score" in blob or (not finishing and "attempt" in blob) or (
                finishing and (attempt.get("attemptId") != values[":id"] or attempt.get("status") != values[":started"])
            ):
                raise ConditionalFailure()
            blob["attempt"] = deepcopy(values[":attempt"])
            if finishing:
                blob.update(score=values[":score"], scoreUpdatedAt=values[":now"])
            self.writes += 1

    def query(self, **kwargs):
        assert kwargs["ConsistentRead"] is True
        return {"Items": [deepcopy(self.player)] if self.player else []}


@pytest.fixture(autouse=True)
def conditional_exception(monkeypatch):
    monkeypatch.setattr(stack, "ClientError", ConditionalFailure)


def start(table, attempt_id="request-1", **extra):
    return stack.store_score("ABCDEF", table, {
        "gameId": "ABCDEF", "playerId": 7, "action": "start", "attemptId": attempt_id, **extra,
    }, META)


def test_start_snapshot_and_idempotent_retry_after_config_change():
    table = AttemptTable()
    first = start(table)
    assert first == {
        "ok": True, "gameId": "ABCDEF", "playerId": 7, "attemptId": "request-1",
        "status": "started", "configVersion": "1",
        "startedAt": table.player["type"]["STACK"]["attempt"]["startedAt"],
        "configSnapshot": DEFAULT_CONFIG,
    }
    attempt = deepcopy(table.player["type"]["STACK"]["attempt"])
    assert attempt["startedAt"].endswith("Z")
    assert attempt["configSnapshot"] == DEFAULT_CONFIG
    changed = {**META, "type": {"STACK": {**DEFAULT_CONFIG, "initialSpeed": 3}}}
    retry = stack.store_score("ABCDEF", table, {
        "playerId": 7, "action": "start", "attemptId": "request-1",
    }, changed)
    assert retry == first
    assert retry["configSnapshot"]["initialSpeed"] == DEFAULT_CONFIG["initialSpeed"]
    assert table.player["type"]["STACK"]["attempt"] == attempt
    assert table.writes == 1
    assert start(table, "request-2")["error"] == "AttemptAlreadyStarted"


@pytest.mark.parametrize("same_id", [True, False])
def test_concurrent_starts_are_atomic(same_id):
    table = AttemptTable(concurrent=True)
    ids = ["same-id", "same-id" if same_id else "other-id"]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda attempt_id: start(table, attempt_id), ids))
    assert table.writes == 1
    if same_id:
        assert all(result["ok"] for result in results)
        assert results[0] == results[1]
    else:
        assert sorted(result["ok"] for result in results) == [False, True]
        assert next(result for result in results if not result["ok"])["error"] == "AttemptAlreadyStarted"


@pytest.mark.parametrize("attempt_id", [None, "", 123, True, "x" * 129, "space id", "id\n", "ñ"])
def test_invalid_attempt_id_never_consumes(attempt_id):
    table = AttemptTable()
    assert start(table, attempt_id)["error"] == "InvalidAttemptId"
    assert table.writes == 0


@pytest.mark.parametrize("field", ["score", "timer", "configSnapshot", "clientRequestId"])
def test_start_rejects_untrusted_extra_fields(field):
    table = AttemptTable()
    assert start(table, **{field: 100})["error"] == "InvalidStackFields"
    assert table.writes == 0


def test_finish_without_start_and_invalid_finish_never_writes():
    table = AttemptTable()
    payload = {"playerId": 7, "action": "finish", "attemptId": "request-1", "placements": []}
    assert stack.store_score("ABCDEF", table, payload, META)["error"] == "AttemptNotStarted"
    assert table.writes == 0
    start(table)
    before = deepcopy(table.player)
    assert stack.store_score("ABCDEF", table, payload, META)["error"] == "InvalidPlacements"
    assert table.player == before
    assert "score" not in table.player["type"]["STACK"]


def test_missing_player_is_not_created():
    table = AttemptTable()
    table.player = None
    assert start(table)["error"] == "PlayerNotFound"
    assert table.writes == 0


def test_player_deleted_between_read_and_write_is_not_recreated():
    table = AttemptTable()
    original_update = table.update_item

    def delete_then_update(**kwargs):
        table.player = None
        return original_update(**kwargs)

    table.update_item = delete_then_update
    assert start(table)["error"] == "PlayerNotFound"
    assert table.player is None


def test_invalid_authoritative_config_never_consumes():
    table = AttemptTable()
    result = stack.store_score("ABCDEF", table, {
        "playerId": 7, "action": "start", "attemptId": "request-1",
    }, {**META, "type": {"STACK": {"initialSpeed": float("nan")}}})
    assert result["error"] == "InvalidStackConfig"
    assert table.writes == 0


def test_snapshot_uses_server_configuration_not_defaults_only():
    table = AttemptTable()
    result = stack.store_score("ABCDEF", table, {
        "playerId": 7, "action": "start", "attemptId": "custom-config",
    }, {**META, "type": {"STACK": {**DEFAULT_CONFIG, "initialSpeed": 3, "perfectTolerance": 0.04}}})
    assert result["ok"] is True
    assert table.player["type"]["STACK"]["attempt"]["configSnapshot"] == {
        **DEFAULT_CONFIG, "initialSpeed": Decimal("3"), "perfectTolerance": Decimal("0.04"),
    }


@pytest.mark.parametrize("payload,error", [
    ({"action": "restart"}, "InvalidStackAction"),
    ({"action": None}, "InvalidStackAction"),
    ({"playerId": 0}, "InvalidPlayerId"),
    ({"playerId": True}, "InvalidPlayerId"),
    ({"playerId": "7"}, "InvalidPlayerId"),
])
def test_invalid_operation_never_consumes(payload, error):
    table = AttemptTable()
    assert start(table, **payload)["error"] == error
    assert table.writes == 0


def test_existing_zero_score_blocks_attempt():
    table = AttemptTable()
    table.player["type"]["STACK"]["score"] = 0
    assert start(table)["error"] == "AttemptAlreadyStarted"
    assert table.writes == 0


def test_assignment_reads_never_start_resume_or_invent_score():
    table = AttemptTable()
    assert public_assignment(table.player)["attempt"] is None
    start(table)
    before = deepcopy(table.player)
    assignment = public_assignment(table.player)
    assert assignment["attempt"]["status"] == "started"
    assert set(assignment["attempt"]) == {"attemptId", "status", "startedAt", "configVersion"}
    assert "score" not in assignment
    assert "configSnapshot" not in assignment["attempt"]
    assert table.player == before


def test_nonconditional_storage_failure_is_propagated():
    class StorageFailure(ConditionalFailure):
        response = {"Error": {"Code": "ProvisionedThroughputExceededException"}}

    table = MagicMock()
    table.get_item.return_value = {"Item": {"type": {"STACK": {}}}}
    table.update_item.side_effect = StorageFailure()
    with pytest.raises(StorageFailure):
        start(table)


def placement(axis="x", offset=0, perfect=True):
    return {"axis": axis, "offset": offset, "perfect": perfect}


HISTORY = [placement(), placement("z", -4, False)]


def finish(table, placements=None, attempt_id="request-1", meta=META, **extra):
    return stack.store_score("ABCDEF", table, {
        "gameId": "ABCDEF", "playerId": 7, "action": "finish", "attemptId": attempt_id,
        "placements": HISTORY if placements is None else placements, **extra,
    }, meta)


def ready_table():
    table = AttemptTable()
    start(table)
    table.player["type"]["STACK"]["attempt"]["startedAt"] = "2026-01-01T00:00:00Z"
    return table


@pytest.mark.parametrize("axis,offset", [("x", 3), ("x", -3), ("z", 2), ("z", -2)])
def test_touching_is_failure_both_axes_signs(axis, offset):
    history = [] if axis == "x" else [placement()]
    assert replay(history + [placement(axis, offset, False)], DEFAULT_CONFIG)[0] == len(history)


@pytest.mark.parametrize("offset", [0.06, -0.06, 0, 0.06000000001, -0.06000000001])
def test_perfect_boundary_snaps_without_shrinking(offset):
    perfect = abs(offset) <= 0.06
    score, _, _ = replay([placement(offset=offset, perfect=perfect), placement("z", 2, False)], DEFAULT_CONFIG)
    assert score == 1
    with pytest.raises(ValueError):
        replay([placement(offset=offset, perfect=not perfect), placement("z", 2, False)], DEFAULT_CONFIG)


def test_inherited_dimensions_and_centre_drift_exact_js_math():
    # JS sees touching; width - abs(offset) would invent an extra success.
    history = [placement("x", 0.07, False), placement("z"),
               placement("x", 2.9299999999999997, False)]
    assert replay(history, DEFAULT_CONFIG)[0] == 2
    # On Z the exact intersection instead retains a binary64 sliver.
    history = [placement(), placement("z", 0.07, False), placement(),
               placement("z", 1.93, False), placement("x", -4, False)]
    assert replay(history, DEFAULT_CONFIG)[0] == 4
    # Perfects inherit both dimensions unchanged, despite centre drift.
    history = [placement("x", -0.7, False), placement("z", 0.5, False),
               placement("x", 0.03, True), placement("z", 1.5, False)]
    assert replay(history, DEFAULT_CONFIG)[0] == 3


@pytest.mark.parametrize("history", [
    [], None, {}, "bad", [None], [placement("z")], [placement(offset=True)],
    [placement(offset="0")], [placement(offset=float("nan"))], [placement(offset=float("inf"))],
    [placement(offset=-float("inf"))], [placement(offset=10**400)],
    [placement(perfect=1)], [placement(axis=True)], [placement(axis=["x"])],
    [{**placement(), "unknown": 1}], [{"axis": "x", "offset": 0}],
    [placement(offset=4.00001, perfect=False)], [placement()],
    [placement(offset=4, perfect=True)], [placement(offset=-4, perfect=False), placement("z")],
])
def test_invalid_histories_never_write(history):
    table = ready_table()
    payload = {"playerId": 7, "action": "finish", "attemptId": "request-1", "placements": history}
    assert stack.store_score("ABCDEF", table, payload, META)["error"] == "InvalidPlacements"
    assert table.writes == 1


def test_no_arbitrary_history_cap_and_canonical_numeric_hash():
    history = [placement("x" if i % 2 == 0 else "z") for i in range(10001)]
    score, minimum, digest = replay(history + [placement("z", -4, False)], DEFAULT_CONFIG)
    assert score == 10001 and minimum > 10000 and len(digest) == 64
    assert replay(HISTORY, DEFAULT_CONFIG)[2] == replay(
        [{"perfect": True, "offset": -0.0, "axis": "x"}, placement("z", -4.0, False)], DEFAULT_CONFIG,
    )[2]


def test_finish_snapshot_atomic_idempotent_and_assignment_recovery():
    table = ready_table()
    changed = {**META, "type": {"STACK": {**DEFAULT_CONFIG, "perfectTolerance": 0}}}
    history = [placement(offset=0.04), placement("z", -4, False)]
    first = finish(table, history, meta=changed)
    assert first["ok"] and first["score"] == 1 and first["rank"] == 1 and first["status"] == "finished"
    assert first["scoreUpdatedAt"] == first["finishedAt"]
    assert finish(table, history, meta={}) == first
    assert start(table) == first
    assert finish(table, [placement(offset=-4, perfect=False)])["error"] == "AttemptConflict"
    assert finish(table, attempt_id="another")["error"] == "AttemptConflict"
    assert start(table, "another")["error"] == "AttemptAlreadyStarted"
    assignment = public_assignment(table.player, table)
    assert assignment["attempt"]["status"] == "finished"
    assert assignment["attempt"]["finishedAt"] == first["finishedAt"]
    assert assignment["score"] == 1 and assignment["rank"] == 1
    blob = table.player["type"]["STACK"]
    assert len(blob["attempt"]["evidenceHash"]) == 64
    assert "placements" not in str(blob)
    assert table.writes == 2


def test_dynamodb_decimal_roundtrip_keeps_float_evidence_hash_and_current_rank():
    import json
    from stand_common.utils import _json_sanitize

    table = ready_table()
    # Payload numbers are decoded from JSON, not read from DynamoDB. Only the
    # snapshot, participant identity and final score undergo SDK Decimal reads.
    history = [placement("x", 0.07, False), placement("z", -0.13, False),
               placement("x", -4, False)]
    first = finish(table, json.loads(json.dumps(history)))
    assert first["ok"] and first["score"] == 2 and first["rank"] == 1
    table.player = json.loads(json.dumps(_json_sanitize(table.player)),
                              parse_float=Decimal, parse_int=Decimal)
    assert isinstance(table.player["playerId"], Decimal)
    assert isinstance(table.player["type"]["STACK"]["score"], Decimal)
    assert isinstance(table.player["type"]["STACK"]["attempt"]["configSnapshot"]["initialSpeed"], Decimal)
    retry = finish(table, json.loads(json.dumps(history)))
    assert retry == first
    assert table.writes == 2
    rival = ranked_player(8, 3)
    table.query = MagicMock(side_effect=[
        {"Items": [rival], "LastEvaluatedKey": {"gameId": "ABCDEF", "playerId": Decimal(8)}},
        {"Items": [deepcopy(table.player)]},
    ])
    recovered = public_assignment(table.player, table)
    assert recovered["score"] == 2 and recovered["rank"] == 2
    assert _json_sanitize(recovered)["playerId"] == 7
    assert all(call.kwargs["ConsistentRead"] is True for call in table.query.call_args_list)
    assert "ExclusiveStartKey" in table.query.call_args.kwargs
    assert "placements" not in table.player["type"]["STACK"]["attempt"]


@pytest.mark.parametrize("elapsed,accepted", [(0, False), (-1, False), (2.219, False), (2.22, True), (3, True)])
def test_temporal_minimum_includes_movement_and_placing(monkeypatch, elapsed, accepted):
    from datetime import datetime, timedelta, timezone
    table = ready_table()
    when = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=elapsed)
    monkeypatch.setattr(stack, "_iso_now", lambda: when.isoformat())
    result = finish(table)
    assert result["ok"] is accepted
    assert table.writes == (2 if accepted else 1)


def test_temporal_speed_progression_and_failure_movement():
    _, minimum, _ = replay([placement(), placement("z"), placement("x", 4, False)], DEFAULT_CONFIG)
    assert minimum == pytest.approx(4 / 2 + 0.22 + 4 / 2.08 + 0.22 + 8 / 2.16)


@pytest.mark.parametrize("same", [True, False])
def test_concurrent_finish_readback_same_evidence_or_conflict(same):
    table = ready_table()
    table.reads = 0
    table.barrier = Barrier(2)
    histories = [HISTORY, HISTORY if same else [placement(offset=-4, perfect=False)]]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda history: finish(table, history), histories))
    assert table.writes == 2
    if same:
        assert results[0] == results[1] and all(result["ok"] for result in results)
    else:
        assert sorted(result["ok"] for result in results) == [False, True]
        assert next(result for result in results if not result["ok"])["error"] == "AttemptConflict"


def test_finish_deleted_during_write_does_not_recreate():
    table = ready_table()
    original = table.update_item

    def delete(**kwargs):
        table.player = None
        original(**kwargs)

    table.update_item = delete
    assert finish(table)["error"] == "PlayerNotFound"
    assert table.player is None


def test_finish_rank_failure_does_not_lose_persisted_result():
    table = ready_table()
    table.query = MagicMock(side_effect=RuntimeError("unavailable"))
    result = finish(table)
    assert result["ok"] and result["rank"] is None
    assert finish(table) == result
    assert table.writes == 2


def test_zero_finish_is_a_real_ranked_result():
    table = ready_table()
    result = finish(table, [placement(offset=-4, perfect=False)])
    assert result["ok"] and result["score"] == 0 and result["rank"] == 1
    assert public_assignment(table.player, table)["score"] == 0
    assert start(table, "second")["error"] == "AttemptAlreadyStarted"


@pytest.mark.parametrize("extra", [
    {"score": 100}, {"timer": 1}, {"configSnapshot": DEFAULT_CONFIG}, {"finishedAt": "2026-01-01"},
])
def test_finish_rejects_extra_untrusted_fields_before_writing(extra):
    table = ready_table()
    assert finish(table, **extra)["error"] == "InvalidStackFields"
    assert table.writes == 1


def test_finish_nonconditional_storage_error_is_not_hidden():
    class StorageFailure(ConditionalFailure):
        response = {"Error": {"Code": "ProvisionedThroughputExceededException"}}

    table = ready_table()
    table.update_item = MagicMock(side_effect=StorageFailure())
    with pytest.raises(StorageFailure):
        finish(table)
    assert table.player["type"]["STACK"]["attempt"]["status"] == "started"


def test_range_floating_tolerance_is_small_and_does_not_change_perfect():
    assert replay([placement(offset=-4 - 5e-10, perfect=False)], DEFAULT_CONFIG)[0] == 0
    with pytest.raises(ValueError):
        replay([placement(offset=-4 - 5e-8, perfect=False)], DEFAULT_CONFIG)


def test_timing_uses_captured_speed_not_current_configuration(monkeypatch):
    table = ready_table()
    monkeypatch.setattr(stack, "_iso_now", lambda: "2026-01-01T00:00:01Z")
    changed = {**META, "type": {"STACK": {**DEFAULT_CONFIG, "initialSpeed": 5}}}
    assert finish(table, meta=changed)["error"] == "InvalidAttemptTime"
    assert table.writes == 1


def ranked_player(pid, score=3, timestamp="2026-01-01T00:00:01Z", status="finished", **identity):
    return {"gameId": "ABCDEF", "playerId": pid, **identity, "type": {"STACK": {
        "score": score, "attempt": {"status": status, "finishedAt": timestamp},
    }}}


def test_ranking_all_pages_descending_deterministic_ties_safe_display():
    table = MagicMock()
    pages = [
        {"Items": [ranked_player(7, 4, email="secret@example.com"), ranked_player(3),
                   ranked_player(1, 100, status="started"), ranked_player(2, True)],
         "LastEvaluatedKey": {"gameId": "ABCDEF", "playerId": 7}},
        {"Items": [ranked_player(9, 10, instagramUsername="ana", instagramProfilePicUrl="https://avatar"),
                   ranked_player(4), ranked_player(5, timestamp="2026-01-01T00:00:00Z"),
                   ranked_player(6, 0), ranked_player(8, -1), ranked_player(10, 3.5),
                   ranked_player(11, timestamp="bad"), ranked_player(12, Decimal("NaN")),
                   ranked_player(13, 1, instagramUsername="secret@example.com")]},
    ]
    table.query.side_effect = deepcopy(pages)
    result = get_ranking("ABCDEF", table, 4)["results"]
    assert [p["playerId"] for p in result] == [9, 7, 5, 3]
    assert [p["rank"] for p in result] == [1, 2, 3, 4]
    assert result[0]["username"] == "@ana" and result[0]["profilePicUrl"] == "https://avatar"
    assert result[1]["username"] == "Jugador 7"
    assert "secret" not in str(result)
    assert table.query.call_args.kwargs["ExclusiveStartKey"] == pages[0]["LastEvaluatedKey"]
    table.query.side_effect = deepcopy(pages)
    all_results = get_ranking("ABCDEF", table, None)["results"]
    assert [p["playerId"] for p in all_results] == [9, 7, 5, 3, 4, 13, 6]


def test_ranking_uses_profile_photo_resolver():
    table = MagicMock()
    player = ranked_player(9, 10, instagramUsername="ana")
    player["participantPhotoKey"] = "players/ABCDEF/9/profile.png"
    table.query.return_value = {"Items": [player]}
    result = get_ranking(
        "ABCDEF", table, 10,
        profile_pic_url_fn=lambda item: f"signed:{item['participantPhotoKey']}",
    )["results"]
    assert result[0]["profilePicUrl"] == "signed:players/ABCDEF/9/profile.png"
