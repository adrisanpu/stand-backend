"""Common close/reactivate/archive contract, including durable prize recovery."""
from copy import deepcopy
from unittest.mock import patch

import pytest

from stand_common import game_lifecycle as lifecycle, stack_prizes as prizes
from tests.test_stack_prize_rules import (
    rig, meta, player, finish, ATTEMPT, PLAYER, process_grants,
)
from tests.test_game_players import game, event, response_body


def task(dynamo):
    return next(row for row in prizes.records(dynamo, "ABC123") if row.get("kind") == "lifecycle"
                and row.get("work") == "pending")


def awards(dynamo):
    return [row for row in prizes.records(dynamo, "ABC123") if row["id"].startswith("award_")]


@pytest.mark.parametrize("game_type", [
    "EMPAREJA2", "T1MER", "RULET4", "L3TRAS", "CUPIDO",
    "SEMAFORO", "INFOCARDS", "IMPOSTOR",
])
def test_common_lifecycle_preserves_players_and_never_draws_raffles(rig, game_type):
    saved = meta(rig)
    saved["gameType"] = game_type
    saved.pop("stackPrizeProgram")
    rig.Table("games").put_item(Item=saved)
    original = player(rig)
    closing = lifecycle.change_active(rig, saved, False)
    assert closing["isActive"] is False and closing["lifecycleState"] == "closing"
    first_task = task(rig)
    lifecycle.change_active(rig, closing, False)
    assert len(prizes.records(rig, "ABC123")) == 1
    with pytest.raises(prizes.PrizeError, match="GameClosing"):
        lifecycle.change_active(rig, closing, True)
    lifecycle.finish(rig, first_task)
    assert meta(rig)["lifecycleState"] == "inactive"
    lifecycle.change_active(rig, meta(rig), False)
    assert len(prizes.records(rig, "ABC123")) == 1
    lifecycle.change_active(rig, meta(rig), True)
    assert meta(rig)["isActive"] is True
    assert player(rig) == original
    assert awards(rig) == []
    assert not meta(rig).get("raffleWinners")


def test_stack_waits_for_attempts_and_allocation_before_reactivation(rig, monkeypatch):
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    lifecycle.change_active(rig, meta(rig), False)
    closing_task = task(rig)
    lifecycle.finish(rig, closing_task)
    assert meta(rig)["stackPrizeState"] == "closing"
    with pytest.raises(prizes.PrizeError, match="StackClosing"):
        prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    finish(rig)
    lifecycle.finish(rig, closing_task)
    assert meta(rig)["stackPrizeState"] == "closed"
    assert meta(rig)["lifecycleState"] == "closing"
    with pytest.raises(prizes.PrizeError, match="GameClosing"):
        lifecycle.change_active(rig, meta(rig), True)
    process_grants(rig, monkeypatch)
    lifecycle.finish(rig, closing_task)
    assert meta(rig)["lifecycleState"] == "inactive"
    snapshot, stock = player(rig), deepcopy(meta(rig)["stackPrizeStock"])
    lifecycle.change_active(rig, meta(rig), True)
    assert meta(rig)["stackPrizeState"] == "open"
    assert player(rig) == snapshot and meta(rig)["stackPrizeStock"] == stock
    with pytest.raises(prizes.PrizeError):
        prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    previous = {row["id"] for row in awards(rig)}
    new = deepcopy(PLAYER)
    new["playerId"] = 2
    rig.Table("players").put_item(Item=new)
    prizes.start(rig, meta(rig), new, {**ATTEMPT, "attemptId": "a2"})
    started = rig.Table("players").get_item(Key={"gameId": "ABC123", "playerId": 2})["Item"]
    prizes.finish(rig, meta(rig), started, {
        **started["type"]["STACK"]["attempt"], "status": "finished",
        "finishedAt": "2026-10-26T12:03:00Z",
    }, 99)
    lifecycle.change_active(rig, meta(rig), False)
    second_task = task(rig)
    assert second_task["id"] != closing_task["id"]
    lifecycle.finish(rig, closing_task)
    assert meta(rig)["lifecycleState"] == "closing"
    lifecycle.finish(rig, second_task)
    process_grants(rig, monkeypatch)
    lifecycle.finish(rig, second_task)
    assert meta(rig)["lifecycleState"] == "inactive"
    assert previous.issubset({row["id"] for row in awards(rig)})
    assert len({(row["playerId"], row["prize"]["id"]) for row in awards(rig)}) == len(awards(rig))
    assert any(row["playerId"] == 2 for row in awards(rig))


def test_archive_is_conditional_and_retains_awards_and_history(rig):
    with pytest.raises(prizes.PrizeError, match="GameMustBeInactive"):
        lifecycle.archive(rig, meta(rig))
    lifecycle.change_active(rig, meta(rig), False)
    saved = meta(rig)
    saved.update(gameType="T1MER")
    rig.Table("games").put_item(Item=saved)
    lifecycle.finish(rig, task(rig))
    with pytest.raises(prizes.PrizeError, match="GameMustBeInactive"):
        lifecycle.archive(rig, {**meta(rig), "isActive": True})
    original = player(rig)
    lifecycle.archive(rig, meta(rig))
    assert meta(rig)["archivedAt"]
    assert player(rig) == original
    with pytest.raises(prizes.PrizeError, match="GameArchived"):
        lifecycle.change_active(rig, meta(rig), True)


def test_archiving_during_close_stops_started_attempts_and_prize_calculation(rig):
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    lifecycle.change_active(rig, meta(rig), False)
    closing_task = task(rig)

    lifecycle.archive(rig, meta(rig))
    archived = meta(rig)

    assert archived["archivedAt"]
    assert archived["isActive"] is False
    assert archived["lifecycleState"] == "inactive"
    assert archived["stackPrizeState"] == "closed"
    assert player(rig)["type"]["STACK"]["attempt"]["status"] == "started"
    lifecycle.finish(rig, closing_task)
    assert not awards(rig)
    assert not any(row.get("kind") == "lifecycle" and row.get("work") == "pending"
                   for row in prizes.records(rig, "ABC123"))


def test_native_ranking_keeps_previous_winners_and_consumed_stock(rig):
    saved = meta(rig)
    saved.update(gameType="T1MER", prizeCatalog={"ranking": {"name": "Medal", "topX": 1, "limit": 2}})
    rig.Table("games").put_item(Item=saved)
    first = player(rig)
    first["type"] = {"T1MER": {"score": 50, "timer": 10}}
    rig.Table("players").put_item(Item=first)
    lifecycle.change_active(rig, meta(rig), False)
    lifecycle.finish(rig, task(rig))
    old_award = awards(rig)[0]
    lifecycle.change_active(rig, meta(rig), True)
    better = {**deepcopy(first), "playerId": 2, "type": {"T1MER": {"score": 10, "timer": 10}}}
    rig.Table("players").put_item(Item=better)
    lifecycle.change_active(rig, meta(rig), False)
    lifecycle.finish(rig, task(rig))
    assert {row["playerId"] for row in awards(rig)} == {1, 2}
    assert old_award in awards(rig)
    lifecycle.change_active(rig, meta(rig), True)
    rig.Table("players").put_item(Item={**deepcopy(better), "playerId": 3,
                                      "type": {"T1MER": {"score": 0, "timer": 10}}})
    lifecycle.change_active(rig, meta(rig), False)
    lifecycle.finish(rig, task(rig))
    assert len(awards(rig)) == 2
    lifecycle.archive(rig, meta(rig))
    prizes.redeem(rig, "ABC123", 1, old_award["id"])
    with pytest.raises(prizes.PrizeError, match="AlreadyDelivered"):
        prizes.redeem(rig, "ABC123", 1, old_award["id"])


def test_native_validation_and_manual_raffle_preserve_collected_prizes(rig):
    saved = meta(rig)
    saved.update(gameType="EMPAREJA2", prizeCatalog={
        "validation": {"name": "Pair", "limit": 1}, "raffle": {"name": "Draw", "limit": 1},
    }, raffleWinners=[{"playerId": 1, "wonAt": "2026-01-01T00:00:00Z"}])
    rig.Table("games").put_item(Item=saved)
    first = player(rig)
    first.update(validated=True, validatedAt="2026-01-01T00:00:00Z",
                 type={"EMPAREJA2": {"validationPrizeDelivered": True}})
    rig.Table("players").put_item(Item=first)
    lifecycle.change_active(rig, meta(rig), False)
    lifecycle.finish(rig, task(rig))
    states = {row["condition"]: row["state"] for row in awards(rig)}
    assert states == {"validation": "delivered", "raffle": "available"}


@pytest.mark.parametrize("game_type", [
    "EMPAREJA2", "T1MER", "STACK", "RULET4", "L3TRAS",
    "CUPIDO", "SEMAFORO", "INFOCARDS", "IMPOSTOR",
])
def test_native_confirmed_award_is_durable_and_obeys_configured_limit(rig, monkeypatch, game_type):
    from unittest.mock import MagicMock

    saved = meta(rig)
    saved.update(gameType=game_type, prizeCatalog={
        "validation": {"name": "Pair", "limit": 1},
    })
    participant = player(rig)
    participant["type"] = {game_type: {}}
    table = MagicMock()
    table.get_item.return_value = {}
    transaction = MagicMock()
    queued = MagicMock(return_value=True)
    monkeypatch.setattr(lifecycle.prizes, "table", lambda _dynamo: table)
    monkeypatch.setattr(lifecycle.prizes, "records", lambda _dynamo, _game_id: [])
    monkeypatch.setattr(lifecycle.prizes, "transact", transaction)
    monkeypatch.setattr(lifecycle.prizes, "enqueue", queued)

    award = lifecycle.issue_native_award(object(), saved, participant, "validation")

    assert award["state"] == "available"
    assert award["emailState"] == "pending"
    assert award["work"] == "pending"
    assert award["dueAt"] == 0
    transaction.assert_called_once()
    update = transaction.call_args.args[1][0]["Update"]
    assert "nativePrizeClaim_" in update["ExpressionAttributeNames"]["#claim"]
    assert "#claim < :limit" in update["ConditionExpression"]
    queued.assert_called_once_with(award)

    monkeypatch.setattr(lifecycle.prizes, "records", lambda _dynamo, _game_id: [
        {"prize": award["prize"]},
    ])
    transaction.reset_mock()
    assert lifecycle.issue_native_award(object(), saved, {
        **participant, "playerId": 2,
    }, "validation") is None
    transaction.assert_not_called()


def test_owner_api_requires_inactive_before_archive_and_checks_owner(rig):
    with patch.object(game, "games_table", rig.Table("games")), patch.object(game, "dynamodb", rig):
        assert game._handle_delete(event("DELETE", "/v1/game", {"gameId": "ABC123"}))["statusCode"] == 409
        assert game._handle_delete(event("DELETE", "/v1/game", {"gameId": "ABC123"}, owner="other"))["statusCode"] == 404
        response = game._handle_put(event("PUT", "/v1/game", {"gameId": "ABC123", "isActive": False}))
        assert response["statusCode"] == 200
        assert response_body(response)["game"]["lifecycleState"] == "closing"


def test_stale_reactivation_and_archive_cannot_overwrite_a_new_close(rig):
    saved = meta(rig)
    saved.update(isActive=False, lifecycleState="inactive")
    rig.Table("games").put_item(Item=saved)
    stale = meta(rig)
    lifecycle.change_active(rig, stale, True)
    with pytest.raises(prizes.PrizeError, match="StackPrizeConflict"):
        lifecycle.archive(rig, stale)
    assert meta(rig)["isActive"] is True


def test_archived_events_are_excluded_from_all_owner_list_pages():
    from unittest.mock import MagicMock
    table = MagicMock()
    table.query.side_effect = [
        {"Items": [{"gameId": "A", "archivedAt": "2026-01-01"}], "LastEvaluatedKey": {"gameId": "A"}},
        {"Items": [{"gameId": "B", "gameName": "Visible", "gameType": "T1MER"}]},
    ]
    with patch.object(game, "games_table", table):
        response = game._handle_get(event("GET", "/v1/game"))
    assert response_body(response)["count"] == 1
    assert [row["gameId"] for row in response_body(response)["games"]] == ["B"]


def test_legacy_inactive_event_can_be_archived_immediately(rig):
    saved = meta(rig)
    saved.update(gameType="T1MER", isActive=False)
    rig.Table("games").put_item(Item=saved)

    lifecycle.archive(rig, meta(rig))
    assert meta(rig)["archivedAt"]
    assert meta(rig)["lifecycleState"] == "inactive"
    assert not awards(rig)


@pytest.mark.parametrize("old_state,expected", [("confirmed", "confirmed"), ("pending", "reconciliation_required"), (None, "queued")])
def test_legacy_kivly_rewards_never_replay_confirmed_or_uncertain_writes(rig, monkeypatch, old_state, expected):
    monkeypatch.setenv("KIVLY_REGISTRATION_TABLE", "registrations")
    saved = meta(rig)
    saved.pop("stackPrizeProgram")
    saved["kivlyReward"] = {"id": "r1", "name": "Coffee", "locationId": "loc"}
    rig.Table("games").put_item(Item=saved)
    first = player(rig)
    first["type"]["STACK"] = {"score": 3, "attempt": {
        **ATTEMPT, "status": "finished", "finishedAt": "2026-01-01T00:00:00Z",
    }}
    rig.Table("players").put_item(Item=first)
    if old_state:
        rig.Table("registrations").put_item(Item={
            "id": "reward:ABC123:1", "state": old_state, "rewardId": "r1",
            "locationId": "loc", "customerId": "customer", "name": "Coffee",
        })
    lifecycle.change_active(rig, meta(rig), False)
    lifecycle.finish(rig, task(rig))
    process_grants(rig, monkeypatch)
    assert len(awards(rig)) == 1
    assert awards(rig)[0]["state"] == expected
    assert (awards(rig)[0].get("work") == "pending") == (old_state is None)
    assert rig.Table("registrations").get_item(Key={"id": "reward:ABC123:1"})["Item"]


def test_native_awards_remain_readable_and_redeemable_after_archive(rig, monkeypatch):
    import json
    from tests.test_public_web import public_mod, SECRET, _redeem_event
    from stand_common.utils import issue_prize_token
    saved = meta(rig)
    saved.update(gameType="T1MER", prizeCatalog={"ranking": {"name": "Medal", "topX": 1, "limit": 1}})
    rig.Table("games").put_item(Item=saved)
    first = player(rig)
    first["type"] = {"T1MER": {"score": 2, "timer": 10}}
    rig.Table("players").put_item(Item=first)
    lifecycle.change_active(rig, meta(rig), False)
    lifecycle.finish(rig, task(rig))
    lifecycle.archive(rig, meta(rig))
    monkeypatch.setattr(public_mod, "dynamo_r", rig)
    monkeypatch.setattr(public_mod, "gp_table", rig.Table("players"))
    monkeypatch.setattr(public_mod, "_get_game", lambda _: meta(rig))
    monkeypatch.setattr(public_mod, "_get_player", lambda *_: player(rig))
    result = public_mod._player_response(meta(rig), player(rig), "ABC123", 1, "session")
    assert result["statusCode"] == 200
    assert json.loads(result["body"])["nativePrizes"][0]["name"] == "Medal"
    token = issue_prize_token("ABC123", 1, "a", SECRET, award_id=awards(rig)[0]["id"])
    result = public_mod._handle_redeem(_redeem_event(token))
    assert result["statusCode"] == 200
    assert json.loads(result["body"])["delivered"] is True
    assert public_mod._handle_redeem(_redeem_event(token))["statusCode"] == 409


def test_native_close_cannot_overwrite_reactivation_after_a_concurrent_worker_finishes(rig, monkeypatch):
    saved = meta(rig)
    saved["gameType"] = "T1MER"
    rig.Table("games").put_item(Item=saved)
    lifecycle.change_active(rig, meta(rig), False)
    closing_task = task(rig)

    def concurrent_finish_and_reactivate(*_):
        updated = meta(rig)
        updated.update(isActive=True, lifecycleState="active",
                       stackPrizeRevision=updated["stackPrizeRevision"] + 2)
        rig.Table("games").put_item(Item=updated)

    monkeypatch.setattr(lifecycle, "settle_native", concurrent_finish_and_reactivate)
    with pytest.raises(prizes.PrizeError, match="StackPrizeConflict"):
        lifecycle.finish(rig, closing_task)
    assert meta(rig)["isActive"] is True and meta(rig)["lifecycleState"] == "active"


def test_stack_close_cannot_finalize_a_reopened_cycle(rig, monkeypatch):
    lifecycle.change_active(rig, meta(rig), False)
    transact = prizes.transact

    def concurrent_finish_and_reactivate(dynamo, operations):
        transact(dynamo, operations)
        if any(op.get("Put", {}).get("Item", {}).get("closure") for op in operations):
            updated = meta(rig)
            updated.update(isActive=True, lifecycleState="active", stackPrizeState="open",
                           stackPrizeRevision=updated["stackPrizeRevision"] + 2)
            rig.Table("games").put_item(Item=updated)

    monkeypatch.setattr(prizes, "transact", concurrent_finish_and_reactivate)
    prizes.close(rig, meta(rig), [])
    assert meta(rig)["isActive"] is True and meta(rig)["stackPrizeState"] == "open"


def test_closure_jobs_cover_every_finished_player_not_only_top_100(rig):
    for pid in range(1, 202):
        rig.Table("players").put_item(Item={
            **deepcopy(PLAYER), "playerId": pid, "type": {"STACK": {
                "score": pid, "attempt": {**ATTEMPT, "status": "finished",
                                        "finishedAt": "2026-01-01T00:00:00Z"},
            }},
        })
    lifecycle.change_active(rig, meta(rig), False)
    prizes.close(rig, meta(rig), [])
    tasks = [row for row in prizes.records(rig, "ABC123") if row.get("closure")]
    assert sorted(len(row["winners"]) for row in tasks) == [1, 100, 100]
    assert len({winner["playerId"] for row in tasks for winner in row["winners"]}) == 201


def test_owner_close_api_rejects_excluding_started_attempts(rig):
    import json
    from stand_common import stack_prize_api
    request = event("POST", "/v1/game/stack/prizes", {"gameId": "ABC123", "excludePlayerIds": [1]})
    result = stack_prize_api.handle(request, "POST", rig, lambda _: True, lambda _: b"")
    assert result["statusCode"] == 409
    assert json.loads(result["body"])["error"] == "PendingAttemptsMustFinish"


def test_native_close_uses_the_reward_configuration_snapshotted_when_requested(rig):
    saved = meta(rig)
    saved.update(gameType="EMPAREJA2", prizeCatalog={"validation": {"name": "Original", "limit": 1}})
    rig.Table("games").put_item(Item=saved)
    first = player(rig)
    first.update(validated=True)
    rig.Table("players").put_item(Item=first)
    lifecycle.change_active(rig, meta(rig), False)
    queued = task(rig)
    changed = meta(rig)
    changed["prizeCatalog"]["validation"]["name"] = "Changed"
    rig.Table("games").put_item(Item=changed)
    lifecycle.finish(rig, queued)
    assert awards(rig)[0]["prize"]["name"] == "Original"


def test_legacy_qr_cannot_bypass_native_stock_after_closure(rig, monkeypatch):
    from tests.test_public_web import public_mod, SECRET, _redeem_event
    from stand_common.utils import issue_prize_token
    saved = meta(rig)
    saved.update(gameType="EMPAREJA2", prizeCatalog={"validation": {"name": "Disabled", "limit": 0}})
    rig.Table("games").put_item(Item=saved)
    first = player(rig)
    first.update(validated=True)
    rig.Table("players").put_item(Item=first)
    lifecycle.change_active(rig, meta(rig), False)
    lifecycle.finish(rig, task(rig))
    monkeypatch.setattr(public_mod, "dynamo_r", rig)
    monkeypatch.setattr(public_mod, "gp_table", rig.Table("players"))
    monkeypatch.setattr(public_mod, "_get_game", lambda _: meta(rig))
    monkeypatch.setattr(public_mod, "_get_player", lambda *_: player(rig))
    result = public_mod._handle_redeem(_redeem_event(issue_prize_token("ABC123", 1, "v", SECRET)))
    assert result["statusCode"] == 409
    assert awards(rig) == []
