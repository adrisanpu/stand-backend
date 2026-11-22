import json
from copy import deepcopy

import pytest

from stand_common import game_lifecycle as lifecycle, stack_auto_close, stack_prizes as prizes
from tests.test_game_lifecycle import awards, task as lifecycle_task
from tests.test_reward_conditions import configured, request  # noqa: F401
from tests.test_stack_prize_rules import (  # noqa: F401
    ATTEMPT, PLAYER, delivery_module, finish, meta, player, process_grants, rig,
)


@pytest.fixture
def clock(monkeypatch):
    now = [1000]
    monkeypatch.setattr(stack_auto_close.time, "time", lambda: now[0])
    return now


def save(dynamo, rules, **extra):
    return request(dynamo, {"action": "rules", "rules": rules, "expectedRevision":
                           json.loads(request(dynamo, {}, "GET")["body"])["programRevision"], **extra})


def final_rule(dynamo):
    return [{**deepcopy(meta(dynamo)["stackPrizeProgram"]["rules"][0]),
             "id": "final_top", "condition": "top_final", "topX": 1}]


def schedule(dynamo, minutes=1):
    response = save(dynamo, final_rule(dynamo), autoClose={"hours": 0, "minutes": minutes})
    assert response["statusCode"] == 200, response
    return stack_auto_close.task("ABC123", meta(dynamo)["stackAutoClose"])


@pytest.mark.parametrize("duration", [
    {"hours": 0, "minutes": 0}, {"hours": True, "minutes": 15}, {"hours": 0, "minutes": 60},
    {"hours": -1, "minutes": 30}, {"hours": 1}, {"hours": 1.5, "minutes": 0},
])
def test_invalid_duration_does_not_save_rules_or_enqueue(configured, clock, duration):
    response = save(configured, final_rule(configured), autoClose=duration)
    assert json.loads(response["body"])["error"] == "InvalidStackAutoClose"
    assert meta(configured)["stackPrizeProgram"]["rules"][0]["condition"] == "completed"
    assert prizes.records(configured, "ABC123") == []


def test_requires_final_top_and_preserves_or_replaces_schedule(configured, clock):
    result = save(configured, meta(configured)["stackPrizeProgram"]["rules"], autoClose={"hours": 0, "minutes": 1})
    assert json.loads(result["body"])["error"] == "StackAutoCloseRequiresFinalTop"
    old_revision = json.loads(request(configured, {}, "GET")["body"])["programRevision"]
    row = schedule(configured)
    assert row["dueAt"] == 1060
    clock[0] += 10
    assert schedule(configured) == row
    assert json.loads(save(configured, final_rule(configured))["body"])["autoClose"]["closesAt"] == 1060
    assert request(configured, {"action": "rules", "rules": [], "expectedRevision": old_revision})["statusCode"] == 409
    replacement = schedule(configured, 2)
    assert replacement["dueAt"] == 1130 and replacement["id"] != row["id"]
    assert prizes.records(configured, "ABC123") == [replacement]
    clock[0] = 1060
    lifecycle.auto_close(configured, row)
    assert meta(configured)["isActive"] is True
    assert json.loads(save(configured, [])["body"])["autoClose"] is None
    assert not prizes.records(configured, "ABC123")


@pytest.mark.parametrize("manual", [False, True])
def test_disabling_or_manually_closing_cancels_the_outbox(configured, clock, monkeypatch, manual):
    old_job = schedule(configured)
    if manual:
        lifecycle.change_active(configured, meta(configured), False)
        closing = lifecycle_task(configured)
        assert "finishDeadline" not in closing
        lifecycle.finish(configured, closing)
        process_grants(configured, monkeypatch)
        lifecycle.finish(configured, closing)
        lifecycle.change_active(configured, meta(configured), True)
    else:
        assert json.loads(save(configured, final_rule(configured), autoClose=None)["body"])["autoClose"] is None
    clock[0] = 1060
    lifecycle.auto_close(configured, old_job)
    assert "stackAutoClose" not in meta(configured)
    assert "stackFinishDeadline" not in meta(configured)
    assert meta(configured)["isActive"] is True


def test_worker_applies_deadline_and_grace_without_duplicate_awards(configured, clock, monkeypatch):
    row = schedule(configured)
    worker = delivery_module(monkeypatch, configured)
    key = {k: row[k] for k in ("gameId", "id")}
    worker._process(key)
    assert meta(configured)["isActive"] is True
    prizes.start(configured, meta(configured), player(configured), ATTEMPT)
    legacy = {**deepcopy(PLAYER), "playerId": 2, "type": {"STACK": {"attempt": {**ATTEMPT, "attemptId": "legacy"}}}}
    late = {**deepcopy(legacy), "playerId": 3}
    for saved in (legacy, late):
        configured.Table("players").put_item(Item=saved)
    clock[0] = 1060
    with pytest.raises(prizes.PrizeError, match="StackClosing"):
        prizes.start(configured, meta(configured), player(configured), ATTEMPT)
    assert request(configured, {"action": "catalog", "program": meta(configured)["stackPrizeProgram"]})["statusCode"] == 409
    legacy_attempt = {**legacy["type"]["STACK"]["attempt"], "status": "finished", "finishedAt": "2026-10-26T12:01:00Z"}
    prizes.finish(configured, meta(configured), legacy, legacy_attempt, 10)
    clock[0] = 1100
    worker._process(key)
    worker._process(key)
    closing = lifecycle_task(configured)
    assert closing["finishDeadline"] == 1180
    assert not meta(configured)["isActive"]
    clock[0] = 1179
    lifecycle.finish(configured, closing)
    assert meta(configured)["stackPrizeState"] == "closing"
    finish(configured, 20)
    clock[0] = 1180
    with pytest.raises(prizes.PrizeError, match="StackClosed"):
        prizes.finish(configured, meta(configured), late, legacy_attempt, 999)
    lifecycle.finish(configured, closing)
    assert configured.Table("players").get_item(Key={"gameId": "ABC123", "playerId": 3})["Item"]["type"]["STACK"]["attempt"]["status"] == "excluded"
    process_grants(configured, monkeypatch)
    lifecycle.finish(configured, closing)
    lifecycle.finish(configured, closing)
    assert meta(configured)["lifecycleState"] == "inactive"
    assert [a["playerId"] for a in awards(configured)] == [1]
    grants = [r for r in prizes.records(configured, "ABC123") if r.get("condition") == "top_final" and r.get("kind") == "grant"]
    assert len(grants) == 1


def test_stale_manual_close_cannot_overwrite_automatic_close(configured, clock):
    row = schedule(configured)
    manual_snapshot = meta(configured)
    clock[0] = 1060
    lifecycle.auto_close(configured, row)
    original = lifecycle_task(configured)
    with pytest.raises(prizes.PrizeError, match="StackPrizeConflict"):
        lifecycle.change_active(configured, manual_snapshot, False)
    lifecycle.change_active(configured, meta(configured), False)
    assert prizes.records(configured, "ABC123") == [original]
    assert original["finishDeadline"] == 1180
