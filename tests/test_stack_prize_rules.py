import importlib.util
import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from stand_common import stack_prizes as prizes
from stand_common.utils import issue_prize_token, verify_prize_token
from tests.email_fake import FakeDynamoError
from tests.stack_prize_fake import PrizeDynamo

LEGACY_PROGRAM = {"version": 1, "storefrontUrl": "https://example.kivly.store", "catalog": [
    {"id": "pizza", "source": "stand", "name": "Pizza", "imageKey": None, "limit": None},
    {"id": "coffee", "source": "kivly", "name": "Coffee", "rewardId": "r1", "locationId": "loc"},
], "rules": [
    {"id": "done", "condition": "completed", "prizeIds": ["pizza", "coffee"]},
    {"id": "top", "condition": "top_finish", "topX": 2, "prizeIds": ["pizza", "coffee"]},
    {"id": "final", "condition": "top_final", "topX": 1, "prizeIds": ["coffee"]},
    {"id": "draw", "condition": "raffle", "prizeIds": ["pizza"]},
]}
PROGRAM = prizes.upgrade_program(LEGACY_PROGRAM)
META = {"gameId": "ABC123", "ownerUserId": "owner", "gameType": "STACK", "isActive": True,
        "stackPrizeProgram": PROGRAM}
PLAYER = {"gameId": "ABC123", "playerId": 1, "username": "Player",
          "kivlyMembership": {"locationId": "loc", "customerId": "customer"},
          "type": {"STACK": {}}}
ATTEMPT = {"attemptId": "a1", "status": "started", "startedAt": "2026-10-26T12:00:00Z",
           "configVersion": "1"}


@pytest.fixture
def rig(monkeypatch):
    for key, value in {"GAMES_TABLE": "games", "GAMEPLAYER_TABLE": "players", "STACK_PRIZES_TABLE": "prizes"}.items():
        monkeypatch.setenv(key, value)
    dynamo = PrizeDynamo()
    dynamo.Table("games").put_item(Item=deepcopy(META))
    dynamo.Table("players").put_item(Item=deepcopy(PLAYER))
    monkeypatch.setattr(prizes, "ClientError", FakeDynamoError)
    monkeypatch.setattr(prizes, "BotoCoreError", FakeDynamoError)
    return dynamo


def meta(dynamo):
    return dynamo.Table("games").get_item(Key={"gameId": "ABC123"})["Item"]


def player(dynamo):
    return dynamo.Table("players").get_item(Key={"gameId": "ABC123", "playerId": 1})["Item"]


def finish(dynamo, score=0):
    current = player(dynamo)
    attempt = current["type"]["STACK"]["attempt"]
    return prizes.finish(dynamo, meta(dynamo), current,
                         {**attempt, "status": "finished", "finishedAt": "2026-10-26T12:01:00Z"}, score)


def test_mixed_rule_validation_keeps_sources_and_rejects_identity_change():
    conn = {"locationId": "loc"}
    result = prizes.validate_program(PROGRAM, META, conn, [{"id": "r1", "name": "Coffee"}], True)
    assert len(result["rules"]) == 4
    changed = deepcopy(PROGRAM)
    changed["rules"][0]["groups"][0]["allocations"] = [{"prizeId": "pizza", "quantity": 1}]
    with pytest.raises(prizes.PrizeError, match="StackRuleIdentityChanged"):
        prizes.validate_program(changed, META, conn, [{"id": "r1", "name": "Coffee"}], True)


def v2_program(mode="all"):
    return {**deepcopy(PROGRAM), "version": 2, "rules": [{
        "id": r["id"], "condition": r["condition"],
        **({"topX": r["topX"]} if "topX" in r else {}),
        "mode": mode, "allocations": deepcopy(r["groups"][0]["allocations"]),
    } for r in PROGRAM["rules"]]}


@pytest.mark.parametrize("old", [LEGACY_PROGRAM, v2_program(), v2_program("random_one")])
def test_old_program_save_normalizes_without_rule_identity_changes(old):
    normalized = prizes.upgrade_program(old)
    old_meta = {**META, "stackPrizeProgram": old}
    result = prizes.validate_program(normalized, old_meta, {"locationId": "loc"},
                                     [{"id": "r1", "name": "Coffee"}], True)
    assert result["version"] == 3
    assert result["rules"] == normalized["rules"]
    assert [r["id"] for r in result["rules"]] == [r["id"] for r in old["rules"]]
    assert all("mode" not in r and "allocations" not in r for r in result["rules"])
    assert prizes.upgrade_program(old)["rules"] == normalized["rules"]
    if old["version"] == 2 and old["rules"][0]["mode"] == "random_one":
        assert len(result["rules"][0]["groups"]) == 2
        assert all(len(g["allocations"]) == 1 for g in result["rules"][0]["groups"])


@pytest.mark.parametrize("groups", [
    [], [{}], [{"allocations": []}], [{"allocations": "pizza"}],
    [{"allocations": [{"prizeId": "missing", "quantity": 1}]}],
    [{"allocations": [{"prizeId": "pizza", "quantity": True}]}],
    [{"allocations": [{"prizeId": "pizza", "quantity": 0}]}],
    [{"allocations": [{"prizeId": "pizza", "quantity": 11}]}],
    [{"allocations": [{"prizeId": "pizza", "quantity": 1}] * 2}],
    [{"allocations": [{"prizeId": "pizza", "quantity": 1}]}] * 9,
    [{"allocations": [{"prizeId": "pizza", "quantity": 1}] * 9}],
])
def test_v3_rejects_invalid_groups(groups):
    program = deepcopy(PROGRAM)
    program["rules"] = [{"id": "new-rule", "condition": "completed", "groups": groups}]
    with pytest.raises(prizes.PrizeError, match="InvalidStackRulePrizes"):
        prizes.validate_program(program, META, {"locationId": "loc"},
                                [{"id": "r1", "name": "Coffee"}], True)


def test_v3_allows_same_prize_across_groups_but_rejects_old_rule_fields():
    program = deepcopy(PROGRAM)
    group = {"allocations": [{"prizeId": "pizza", "quantity": 10}]}
    program["rules"] = [{"id": "new-rule", "condition": "completed", "groups": [group] * 8}]
    assert len(prizes.validate_program(program, META, {"locationId": "loc"},
                                      [{"id": "r1", "name": "Coffee"}], True)["rules"][0]["groups"]) == 8
    program["rules"][0]["mode"] = "all"
    with pytest.raises(prizes.PrizeError, match="InvalidStackPrizeRule"):
        prizes.validate_program(program, META, {"locationId": "loc"},
                                [{"id": "r1", "name": "Coffee"}], True)


def test_or_selection_is_uniform_by_group_not_stock_or_group_size(rig, monkeypatch):
    program = deepcopy(PROGRAM)
    program["catalog"] = [
        {"id": pid, "source": "stand", "name": pid, "stockTotal": 1000}
        for pid in ("pizza", "coffee", "empty")
    ]
    program["rules"] = [{"id": "packages", "condition": "completed", "groups": [
        {"allocations": [{"prizeId": "pizza", "quantity": 10},
                         {"prizeId": "coffee", "quantity": 10}]},
        {"allocations": [{"prizeId": "coffee", "quantity": 1}]},
        {"allocations": [{"prizeId": "empty", "quantity": 1}]},
    ]}]
    game = {**meta(rig), "stackPrizeProgram": program, "stackPrizeStock": {
        "pizza": {"total": 1000, "remaining": 1},
        "coffee": {"total": 1000, "remaining": 500},
        "empty": {"total": 1000, "remaining": 0},
    }}
    rig.Table("games").put_item(Item=game)
    calls = []
    def choose(options):
        calls.append(deepcopy(options))
        assert options == [g["allocations"] for g in program["rules"][0]["groups"][:2]]
        return options[0]
    monkeypatch.setattr(prizes.secrets, "choice", choose)
    prizes.allocate_rule(rig, game, PLAYER, program, program["rules"][0], "package-job")
    first_stock = meta(rig)["stackPrizeStock"]
    prizes.allocate_rule(rig, game, PLAYER, program, program["rules"][0], "package-job")
    assert len(calls) == 1
    assert first_stock == meta(rig)["stackPrizeStock"]
    assert first_stock["pizza"]["remaining"] == 0
    assert first_stock["coffee"]["remaining"] == 490
    awards = [r for r in prizes.records(rig, "ABC123") if r["id"].startswith("award_")]
    assert len(awards) == 11


@pytest.mark.parametrize("limited,remaining,expected", [(True, 5, "not_eligible"),
                                                       (False, 0, "out_of_stock")])
def test_unavailable_groups_never_draw_or_award(rig, monkeypatch, limited, remaining, expected):
    program = deepcopy(PROGRAM)
    program["catalog"] = [{**program["catalog"][0], "limit": 1 if limited else None}]
    program["rules"] = [{"id": "empty", "condition": "completed", "groups": [
        {"allocations": [{"prizeId": "pizza", "quantity": 3}]},
    ]}]
    game = {**meta(rig), "stackPrizeProgram": program,
            "stackPrizeStock": {"pizza": {"total": 5, "remaining": remaining}}}
    rig.Table("games").put_item(Item=game)
    choice = MagicMock()
    monkeypatch.setattr(prizes.secrets, "choice", choice)
    prizes.allocate_rule(rig, game, PLAYER, program, program["rules"][0], "empty-job", rank=2)
    prizes.allocate_rule(rig, game, PLAYER, program, program["rules"][0], "empty-job", rank=2)
    choice.assert_not_called()
    rows = prizes.records(rig, "ABC123")
    assert len(rows) == 1 and rows[0]["state"] == expected


@pytest.mark.parametrize("old", [LEGACY_PROGRAM, v2_program("random_one")])
def test_legacy_awards_survive_conversion_without_reaward(rig, old):
    game = {**meta(rig), "stackPrizeProgram": old}
    previous_awards = prizes.award_rows(game, PLAYER, old, {"completed"}, rank=1)
    for row in previous_awards:
        rig.Table("prizes").put_item(Item=row)
    program = prizes.upgrade_program(old)
    game.update(stackPrizeProgram=program, stackPrizeStock=prizes.initial_stock(program, previous_awards))
    rig.Table("games").put_item(Item=game)
    prizes.allocate_rule(rig, game, PLAYER, program, program["rules"][0], "new-format-job", rank=1)
    assert len([r for r in prizes.records(rig, "ABC123") if r["id"].startswith("award_")]) == len(previous_awards)
    assert meta(rig)["stackPrizeStock"] == game["stackPrizeStock"]


def test_old_v2_job_still_delivers_and_retries_idempotently(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    task = prizes.job(meta(rig), "completed", [{"playerId": 1, "rank": 1}])
    task["program"] = v2_program("random_one")
    rig.Table("prizes").put_item(Item=task)
    monkeypatch.setattr(prizes.secrets, "choice", lambda options: options[0])
    worker.grant(task)
    worker.grant(task)
    awards = [r for r in prizes.records(rig, "ABC123") if r["id"].startswith("award_")]
    assert len(awards) == 1 and awards[0]["prize"]["id"] == "pizza"


def test_group_stock_revision_conflict_rolls_back_units_and_ledger(rig, monkeypatch):
    game = meta(rig)
    original_stock = prizes.initial_stock(PROGRAM)
    game["stackPrizeStock"] = original_stock
    rig.Table("games").put_item(Item=game)
    real_transact = prizes.transact
    def conflicting_transact(dynamo, operations):
        real_transact(dynamo, [prizes.game_update(meta(dynamo), "")])
        real_transact(dynamo, operations)
    monkeypatch.setattr(prizes, "transact", conflicting_transact)
    with pytest.raises(prizes.PrizeError, match="StackPrizeConflict"):
        prizes.allocate_rule(rig, game, PLAYER, PROGRAM, PROGRAM["rules"][0], "racing-job")
    assert prizes.records(rig, "ABC123") == []
    assert meta(rig)["stackPrizeStock"] == original_stock
    monkeypatch.setattr(prizes, "transact", real_transact)
    prizes.allocate_rule(rig, meta(rig), PLAYER, PROGRAM, PROGRAM["rules"][0], "racing-job")
    prizes.allocate_rule(rig, meta(rig), PLAYER, PROGRAM, PROGRAM["rules"][0], "racing-job")
    rows = prizes.records(rig, "ABC123")
    assert len(rows) == 3
    assert meta(rig)["stackPrizeStock"]["pizza"]["remaining"] == 999


def test_kivly_prize_image_is_taken_only_from_verified_catalog_not_save_payload():
    program = deepcopy(PROGRAM)
    program["catalog"][1]["imageUrl"] = "https://spoofed.test/image.png"
    result = prizes.validate_program(program, META, {"locationId": "loc"}, [{
        "id": "r1", "name": "Coffee", "imageUrl": "https://cdn.kivly.store/verified.png",
    }], True)
    assert result["catalog"][1]["imageUrl"] == "https://cdn.kivly.store/verified.png"
    no_image = prizes.validate_program(program, META, {"locationId": "loc"},
                                       [{"id": "r1", "name": "Coffee"}], True)
    assert "imageUrl" not in no_image["catalog"][1]


def test_kivly_prize_can_use_an_owned_uploaded_image_and_clear_it(monkeypatch):
    s3 = MagicMock()
    s3.generate_presigned_url.return_value = "https://signed.test/image.png"
    monkeypatch.setattr(prizes.boto3, "client", lambda _: s3)
    monkeypatch.setenv("USER_MEDIA_BUCKET", "media")
    key = "games/owner/ABC123/ranking/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.png"
    program = deepcopy(PROGRAM)
    program["catalog"][1]["imageKey"] = key
    result = prizes.validate_program(program, META, {"locationId": "loc"}, [{
        "id": "r1", "name": "Coffee", "imageUrl": "https://cdn.kivly.store/verified.png",
    }], True)
    assert result["catalog"][1]["imageKey"] == key
    assert prizes.program_view(result, META)["catalog"][1]["imageUrl"] == "https://signed.test/image.png"
    assert prizes.image_url(result["catalog"][1], META) == "https://signed.test/image.png"
    program["catalog"][1]["imageKey"] = None
    cleared = prizes.validate_program(program, META, {"locationId": "loc"}, [{
        "id": "r1", "name": "Coffee", "imageUrl": "https://cdn.kivly.store/verified.png",
    }], True)
    assert prizes.program_view(cleared, META)["catalog"][1]["imageUrl"] is None


@pytest.mark.parametrize("key", [
    "games/other/ABC123/ranking/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.png",
    "games/owner/XYZ789/ranking/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.png",
    "games/staged/owner/ABC123/ranking/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.png",
    "https://untrusted.test/image.png", "", 123,
])
def test_kivly_prize_rejects_unverified_image_keys(key):
    program = deepcopy(PROGRAM)
    program["catalog"][1]["imageKey"] = key
    with pytest.raises(prizes.PrizeError, match="InvalidStackPrizeImage"):
        prizes.validate_program(program, META, {"locationId": "loc"}, [{"id": "r1", "name": "Coffee"}], True)


def test_owner_save_promotes_kivly_upload_and_returns_fresh_image(rig, monkeypatch):
    from stand_common import stack_prize_api
    conn = {"locationId": "loc", "ownerId": "business"}
    client = MagicMock()
    client.list_rewards.return_value = [{"id": "r1", "name": "Coffee"}]
    monkeypatch.setattr(stack_prize_api.registration, "connection", lambda *_: conn)
    monkeypatch.setattr(stack_prize_api.registration, "account_game_settings", lambda *_: {"enabled": True})
    monkeypatch.setattr(stack_prize_api.registration, "_client", lambda *_: client)
    s3 = MagicMock()
    s3.generate_presigned_url.side_effect = lambda _, **kw: "signed:" + kw["Params"]["Key"]
    monkeypatch.setattr(prizes.boto3, "client", lambda _: s3)
    monkeypatch.setenv("USER_MEDIA_BUCKET", "media")
    staged = "games/staged/owner/ABC123/ranking/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.png"
    program = deepcopy(PROGRAM)
    program["catalog"][1]["imageKey"] = staged
    read_image = MagicMock(return_value=b"verified-png")
    event = {"requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
             "body": json.dumps({"gameId": "ABC123", "program": program})}
    response = stack_prize_api.handle(event, "PUT", rig, lambda _: True, read_image)
    assert response["statusCode"] == 200, response
    saved = meta(rig)["stackPrizeProgram"]["catalog"][1]
    assert saved["imageKey"].startswith("games/owner/ABC123/ranking/")
    read_image.assert_called_once_with(staged)
    s3.put_object.assert_called_once_with(
        Bucket="media", Key=saved["imageKey"], Body=b"verified-png", ContentType="image/png",
    )
    assert json.loads(response["body"])["program"]["catalog"][1]["imageUrl"] == "signed:" + saved["imageKey"]


@pytest.mark.parametrize("url", ["http://example.kivly.store", "https://kivly.store.attacker.test",
                                "https://user:pass@example.kivly.store", "https://example.kivly.store/add-to-wallet",
                                "https://example.kivly.store?customer_id=x", "https://example.kivly.store:444",
                                "https://[invalid"])
def test_storefront_rejects_unsafe_or_customer_specific_urls(url):
    with pytest.raises(prizes.PrizeError):
        prizes.storefront(url)


def test_zero_score_finish_persists_grant_job_atomically(rig, monkeypatch):
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    assert finish(rig) == 1
    rows = prizes.records(rig, "ABC123")
    assert len(rows) == 1 and rows[0]["kind"] == "grant"
    assert rows[0]["condition"] == "finish"
    process_grants(rig, monkeypatch)
    awards = [r for r in prizes.records(rig, "ABC123") if r["id"].startswith("award_")]
    assert len(awards) == 4
    assert sum(r["state"] == "available" for r in awards) == 2
    assert sum(r.get("work") == "pending" for r in awards) == 2
    assert player(rig)["type"]["STACK"]["rankAtFinish"] == 1


def test_retried_finish_does_not_change_score_or_grant_again(rig):
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    stale_player, stale_meta = player(rig), meta(rig)
    finish(rig, 3)
    with pytest.raises(prizes.PrizeError, match="StackPrizeConflict"):
        prizes.finish(rig, stale_meta, stale_player, {**ATTEMPT, "status": "finished", "finishedAt": "2026-10-26T12:01:00Z"}, 9)
    assert player(rig)["type"]["STACK"]["score"] == 3
    assert len(prizes.records(rig, "ABC123")) == 1


def test_finish_enqueues_only_after_committing_outbox(rig, monkeypatch):
    queue = MagicMock()
    monkeypatch.setenv("STACK_PRIZES_QUEUE", "queue-url")
    monkeypatch.setattr(prizes.boto3, "client", lambda service: queue)
    def sent(**message):
        key = json.loads(message["MessageBody"])
        row = rig.Table("prizes").get_item(Key=key)["Item"]
        assert row["kind"] == "grant" and row["work"] == "pending"
        assert player(rig)["type"]["STACK"]["attempt"]["status"] == "finished"
    queue.send_message.side_effect = sent
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    finish(rig)
    queue.send_message.assert_called_once()
    assert queue.send_message.call_args.kwargs["QueueUrl"] == "queue-url"


def test_failed_finish_transaction_never_enqueues(rig, monkeypatch):
    enqueue = MagicMock()
    monkeypatch.setattr(prizes, "enqueue", enqueue)
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    stale = meta(rig)
    prizes.transact(rig, [prizes.game_update(meta(rig), "")])
    with pytest.raises(prizes.PrizeError):
        prizes.finish(rig, stale, player(rig),
                      {**ATTEMPT, "status": "finished", "finishedAt": "2026-10-26T12:01:00Z"}, 4)
    enqueue.assert_not_called()


def test_enqueue_failure_keeps_successful_finish_and_recoverable_job(rig, monkeypatch):
    queue, logger = MagicMock(), MagicMock()
    queue.send_message.side_effect = FakeDynamoError("ServiceUnavailable")
    monkeypatch.setenv("STACK_PRIZES_QUEUE", "queue-url")
    monkeypatch.setattr(prizes.boto3, "client", lambda service: queue)
    monkeypatch.setattr(prizes, "log", logger)
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    assert finish(rig) == 1
    task = prizes.records(rig, "ABC123")[0]
    assert task["work"] == "pending" and task["dueAt"] == 0
    logger.assert_called_once()
    worker = delivery_module(monkeypatch, rig)
    queue.send_message.side_effect = None
    worker.dispatch()
    assert queue.send_message.call_count == 2
    assert rig.Table("prizes").get_item(Key={"gameId": task["gameId"], "id": task["id"]})["Item"]["dueAt"] > 0


def test_allocation_immediately_enqueues_only_committed_kivly_awards(rig, monkeypatch):
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    finish(rig)
    sent = []
    def enqueue(row):
        stored = rig.Table("prizes").get_item(Key={"gameId": row["gameId"], "id": row["id"]})["Item"]
        assert stored["state"] == "queued" and stored["work"] == "pending"
        sent.append(row)
        return True
    monkeypatch.setattr(prizes, "enqueue", enqueue)
    process_grants(rig, monkeypatch)
    assert len(sent) == 2 and all(r["prize"]["source"] == "kivly" for r in sent)
    process_grants(rig, monkeypatch)
    assert len(sent) == 2


def test_enqueue_preserves_customer_fifo_and_stable_deduplication(monkeypatch):
    queue = MagicMock()
    monkeypatch.setenv("STACK_PRIZES_QUEUE", "queue-url")
    monkeypatch.setattr(prizes.boto3, "client", lambda service: queue)
    monkeypatch.setattr(prizes.time, "time", lambda: 120)
    row = {"gameId": "ABC123", "id": "award_1", "customerId": "customer"}
    assert prizes.enqueue(row)
    assert prizes.enqueue(row)
    first, second = [call.kwargs for call in queue.send_message.call_args_list]
    assert first == second
    assert prizes.enqueue({**row, "id": "award_2"})
    third = queue.send_message.call_args.kwargs
    assert first["MessageGroupId"] == third["MessageGroupId"]
    assert first["MessageDeduplicationId"] != third["MessageDeduplicationId"]


def test_missing_queue_configuration_is_logged(monkeypatch):
    monkeypatch.delenv("STACK_PRIZES_QUEUE", raising=False)
    logger = MagicMock()
    monkeypatch.setattr(prizes, "log", logger)
    assert not prizes.enqueue({"gameId": "ABC123", "id": "job_1"})
    assert logger.call_args.args[1]["code"] == "MissingQueueConfiguration"


def test_finish_without_completion_rules_does_not_enqueue(rig, monkeypatch):
    game = meta(rig)
    game["stackPrizeProgram"]["rules"] = [PROGRAM["rules"][2]]
    rig.Table("games").put_item(Item=game)
    enqueue = MagicMock()
    monkeypatch.setattr(prizes, "enqueue", enqueue)
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    assert finish(rig) == 1
    enqueue.assert_not_called()
    assert prizes.records(rig, "ABC123") == []


def test_finish_to_delivery_runs_from_queue_without_scheduler(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    queue, client = MagicMock(), MagicMock()
    monkeypatch.setenv("STACK_PRIZES_QUEUE", "queue-url")
    monkeypatch.setattr(prizes.boto3, "client", lambda service: queue)
    messages = []
    queue.send_message.side_effect = lambda **message: messages.append(message["MessageBody"])
    redemptions = []
    client.customer_rewards.side_effect = lambda *_: deepcopy(redemptions)
    def earn(*_):
        redemptions.append({"id": str(len(redemptions)), "reward_id": "r1"})
        return {"success": True}
    client.earn_reward.side_effect = earn
    monkeypatch.setattr(worker.registration, "connection", lambda *_: {"locationId": "loc", "ownerId": "owner"})
    monkeypatch.setattr(worker.registration, "_client", lambda *_: client)
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    finish(rig)
    processed = 0
    while messages:
        processed += 1
        assert processed <= 3
        message = messages.pop(0)
        event = {"Records": [{"messageId": str(processed), "body": message}]}
        assert worker.lambda_handler(event, None) == {"batchItemFailures": []}
        assert worker.lambda_handler(event, None) == {"batchItemFailures": []}
    assert processed == 3
    rows = prizes.records(rig, "ABC123")
    assert sum(r.get("state") == "available" for r in rows) == 2
    assert sum(r.get("state") == "confirmed" and r.get("customerId") == "customer" for r in rows) == 2
    assert not any(r.get("work") == "pending" for r in rows)
    assert client.earn_reward.call_count == 2
    client.customer_rewards.assert_not_called()


def test_rule_snapshot_is_taken_at_start(rig, monkeypatch):
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    changed = meta(rig)
    changed["stackPrizeProgram"]["rules"] = []
    changed["stackPrizeRevision"] += 1
    rig.Table("games").put_item(Item=changed)
    finish(rig)
    process_grants(rig, monkeypatch)
    assert len([r for r in prizes.records(rig, "ABC123") if r["id"].startswith("award_")]) == 4


def test_concurrent_finish_revision_conflict_rolls_back_score_and_awards(rig):
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    stale = meta(rig)
    prizes.transact(rig, [prizes.game_update(meta(rig), "")])
    with pytest.raises(prizes.PrizeError):
        prizes.finish(rig, stale, player(rig),
                      {**ATTEMPT, "status": "finished", "finishedAt": "2026-10-26T12:01:00Z"}, 4)
    assert "score" not in player(rig)["type"]["STACK"]
    assert prizes.records(rig, "ABC123") == []


def test_close_blocks_starts_waits_then_freezes_once(rig):
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    with pytest.raises(prizes.PrizeError, match="StackClosingStarted"):
        prizes.close(rig, meta(rig), [])
    assert meta(rig)["stackPrizeState"] == "closing"
    with pytest.raises(prizes.PrizeError, match="StackClosing"):
        prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    prizes.close(rig, meta(rig), [])
    assert meta(rig)["stackPrizeState"] == "closing"
    finish(rig)
    prizes.close(rig, meta(rig), [])
    assert meta(rig)["stackPrizeState"] == "closed"
    tasks = [r for r in prizes.records(rig, "ABC123") if r.get("kind") == "grant"]
    assert len(tasks) == 2 and any(t["winners"] == [{"playerId": 1, "rank": 1}] for t in tasks)
    prizes.close(rig, meta(rig), [])
    assert len([r for r in prizes.records(rig, "ABC123") if r.get("kind") == "grant"]) == 2


def test_excluded_attempt_cannot_finish_or_receive_final_awards(rig):
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    with pytest.raises(prizes.PrizeError):
        prizes.close(rig, meta(rig), [])
    prizes.close(rig, meta(rig), [1])
    assert player(rig)["type"]["STACK"]["attempt"]["status"] == "excluded"
    with pytest.raises(prizes.PrizeError, match="StackClosed"):
        finish(rig)
    assert not any(r["id"].startswith("award_") for r in prizes.records(rig, "ABC123"))


def test_individual_qr_can_only_redeem_one_stand_award(rig, monkeypatch):
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    finish(rig)
    process_grants(rig, monkeypatch)
    awards = prizes.records(rig, "ABC123")
    awards = [r for r in awards if r["id"].startswith("award_")]
    stand = [r for r in awards if r["prize"]["source"] == "stand"]
    secret = "s" * 32
    token = issue_prize_token("ABC123", 1, "a", secret, award_id=stand[0]["id"])
    claims = verify_prize_token(token, secret)
    assert claims["awardId"] == stand[0]["id"]
    prizes.redeem(rig, "ABC123", 1, claims["awardId"])
    with pytest.raises(prizes.PrizeError, match="AlreadyDelivered"):
        prizes.redeem(rig, "ABC123", 1, claims["awardId"])
    with pytest.raises(prizes.PrizeError, match="NotEligible"):
        prizes.redeem(rig, "ABC123", 2, stand[1]["id"])
    kivly = next(r for r in awards if r["prize"]["source"] == "kivly")
    with pytest.raises(prizes.PrizeError, match="NotEligible"):
        prizes.redeem(rig, "ABC123", 1, kivly["id"])
    assert rig.Table("prizes").get_item(Key={"gameId": "ABC123", "id": stand[1]["id"]})["Item"]["state"] == "available"


def delivery_module(monkeypatch, rig):
    path = Path(__file__).parents[1] / "src" / "stand_stack_prize_delivery" / "lambda_function.py"
    spec = importlib.util.spec_from_file_location("stack_delivery_tests", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "dynamo", rig)
    monkeypatch.setattr(module, "ClientError", FakeDynamoError)
    return module


def process_grants(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    for row in prizes.records(rig, "ABC123"):
        if row.get("kind") == "grant":
            worker.grant(row)


def test_worker_earns_once_without_browser_and_handles_multiple_identical_rewards(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    finish(rig)
    task = next(r for r in prizes.records(rig, "ABC123") if r.get("kind") == "grant")
    worker.grant(task)
    client = MagicMock()
    redemptions = []
    client.customer_rewards.side_effect = lambda *_: deepcopy(redemptions)
    def earn(*_):
        redemptions.append({"id": str(len(redemptions)), "reward_id": "r1"})
        return {"success": True}
    client.earn_reward.side_effect = earn
    monkeypatch.setattr(worker.registration, "connection", lambda *_: {"locationId": "loc", "ownerId": "owner"})
    monkeypatch.setattr(worker.registration, "_client", lambda *_: client)
    for row in prizes.records(rig, "ABC123"):
        if row.get("id", "").startswith("award_") and row["prize"]["source"] == "kivly":
            worker.deliver(row)
            worker.deliver(rig.Table("prizes").get_item(Key={"gameId": "ABC123", "id": row["id"]})["Item"])
    assert client.earn_reward.call_count == 2
    assert len(redemptions) == 2
    assert all(r["state"] == "confirmed" for r in prizes.records(rig, "ABC123")
               if r.get("id", "").startswith("award_") and r["prize"]["source"] == "kivly")
    client.customer_rewards.assert_not_called()


def test_success_ack_confirms_even_when_kivly_redemption_history_is_empty(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    row = next(r for r in prizes.award_rows(META, PLAYER, PROGRAM, {"completed"}, 1)
               if r["prize"]["source"] == "kivly")
    rig.Table("prizes").put_item(Item=row)
    client = MagicMock()
    client.customer_rewards.return_value = []
    client.earn_reward.return_value = {"success": True, "message": "Reward earned"}
    monkeypatch.setattr(worker.registration, "connection", lambda *_: {"locationId": "loc", "ownerId": "owner"})
    monkeypatch.setattr(worker.registration, "_client", lambda *_: client)

    worker.deliver(row)

    current = rig.Table("prizes").get_item(Key={"gameId": row["gameId"], "id": row["id"]})["Item"]
    assert current["state"] == "confirmed"
    assert "redemptionId" not in current
    assert "work" not in current
    client.customer_rewards.assert_not_called()
    assert client.earn_reward.call_count == 1


def test_confirmed_award_is_not_requested_again(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    row = next(r for r in prizes.award_rows(META, PLAYER, PROGRAM, {"completed"}, 1)
               if r["prize"]["source"] == "kivly")
    rig.Table("prizes").put_item(Item=row)
    client = MagicMock()
    client.customer_rewards.return_value = []
    client.earn_reward.return_value = {"success": True}
    monkeypatch.setattr(worker.registration, "connection", lambda *_: {"locationId": "loc", "ownerId": "owner"})
    monkeypatch.setattr(worker.registration, "_client", lambda *_: client)

    worker.deliver(row)
    current = rig.Table("prizes").get_item(Key={"gameId": row["gameId"], "id": row["id"]})["Item"]

    assert current["state"] == "confirmed"
    client.customer_rewards.assert_not_called()
    worker.deliver(current)
    assert client.earn_reward.call_count == 1


def test_uncertain_earn_is_never_replayed(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    row = next(r for r in prizes.award_rows(META, PLAYER, PROGRAM, {"completed"}, 1) if r["prize"]["source"] == "kivly")
    rig.Table("prizes").put_item(Item=row)
    client = MagicMock()
    client.customer_rewards.return_value = []
    client.earn_reward.side_effect = worker.registration.KivlyError("KivlyUnavailable")
    monkeypatch.setattr(worker.registration, "connection", lambda *_: {"locationId": "loc", "ownerId": "owner"})
    monkeypatch.setattr(worker.registration, "_client", lambda *_: client)
    worker.deliver(row)
    current = rig.Table("prizes").get_item(Key={"gameId": "ABC123", "id": row["id"]})["Item"]
    assert current["state"] == "reconciliation_required" and "work" not in current
    worker.deliver(current)
    assert client.earn_reward.call_count == 1


def test_interrupted_send_is_not_repeated(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    row = next(r for r in prizes.award_rows(META, PLAYER, PROGRAM, {"completed"}, 1) if r["prize"]["source"] == "kivly")
    row["state"] = "sending"
    rig.Table("prizes").put_item(Item=row)
    worker.deliver(row)
    assert rig.Table("prizes").get_item(Key={"gameId": "ABC123", "id": row["id"]})["Item"]["state"] == "reconciliation_required"


def test_grant_task_retry_is_idempotent(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    task = prizes.job(META, "raffle", [{"playerId": 1}])
    rig.Table("prizes").put_item(Item=task)
    worker.grant(task)
    worker.grant(task)
    assert len([r for r in prizes.records(rig, "ABC123") if r["id"].startswith("award_")]) == 1


def test_v3_grant_deserialized_from_dynamodb_allocates_once(rig, monkeypatch):
    game = meta(rig)
    game["stackPrizeProgram"] = prizes.upgrade_program(PROGRAM)
    rig.Table("games").put_item(Item=json.loads(json.dumps(game), parse_int=Decimal))
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    finish(rig)
    task = next(r for r in prizes.records(rig, "ABC123") if r.get("kind") == "grant")
    task = json.loads(json.dumps(task, default=int), parse_int=Decimal)
    assert isinstance(task["program"]["rules"][0]["groups"][0]["allocations"][0]["quantity"], Decimal)
    worker = delivery_module(monkeypatch, rig)
    worker.grant(task)
    worker.grant(task)
    awards = [r for r in prizes.records(rig, "ABC123") if r["id"].startswith("award_")]
    assert len(awards) == 4
    assert sum(r["state"] == "available" for r in awards) == 2
    assert sum(r["state"] == "queued" for r in awards) == 2
    assert meta(rig)["stackPrizeStock"]["pizza"]["remaining"] == 998


@pytest.mark.parametrize("quantity", [True, "1", 1.5, Decimal("1.5"),
                                     Decimal("NaN"), Decimal("Infinity"), -1, 11])
def test_award_units_rejects_invalid_quantities(quantity):
    with pytest.raises(prizes.PrizeError, match="InvalidStackPrizeQuantity"):
        prizes.award_units(META, PLAYER, {"id": "done", "condition": "completed"},
                           PROGRAM["catalog"][0], quantity)


def test_stock_is_shared_across_rules_and_shortage_grants_only_remaining_units(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    program = prizes.upgrade_program(PROGRAM)
    program["catalog"][0]["stockTotal"] = 5
    program["rules"] = [
        {"id": "first", "condition": "completed",
         "groups": [{"allocations": [{"prizeId": "pizza", "quantity": 3}]}]},
        {"id": "second", "condition": "completed",
         "groups": [{"allocations": [{"prizeId": "pizza", "quantity": 3}]}]},
    ]
    game = meta(rig)
    game["stackPrizeProgram"] = program
    game["stackPrizeStock"] = {"pizza": {"total": 5, "remaining": 5},
                               "coffee": {"total": 1000, "remaining": 1000}}
    rig.Table("games").put_item(Item=game)
    task = prizes.job(game, "completed", [{"playerId": 1, "rank": 1}])
    task["program"] = program
    rig.Table("prizes").put_item(Item=task)

    worker.grant(task)
    worker.grant(task)

    awards = [r for r in prizes.records(rig, "ABC123") if r.get("id", "").startswith("award_")]
    assert len(awards) == 5
    assert all(r["prize"]["id"] == "pizza" for r in awards)
    assert meta(rig)["stackPrizeStock"]["pizza"] == {"total": 5, "remaining": 0}
    ledgers = [r for r in prizes.records(rig, "ABC123") if r.get("kind") == "allocation"]
    assert len(ledgers) == 2
    assert sorted(sum(p["quantity"] for p in row["awarded"]) for row in ledgers) == [2, 3]


def test_random_one_chooses_only_a_prize_with_stock_and_persists_result(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    program = prizes.upgrade_program(PROGRAM)
    program["rules"] = [{
        "id": "choice", "condition": "completed",
        "groups": [{"allocations": [{"prizeId": "pizza", "quantity": 4}]},
                   {"allocations": [{"prizeId": "coffee", "quantity": 4}]}],
    }]
    game = meta(rig)
    game["stackPrizeProgram"] = program
    game["stackPrizeStock"] = {"pizza": {"total": 5, "remaining": 0},
                               "coffee": {"total": 2, "remaining": 2}}
    rig.Table("games").put_item(Item=game)
    task = prizes.job(game, "completed", [{"playerId": 1, "rank": 1}])
    task["program"] = program
    rig.Table("prizes").put_item(Item=task)
    choices = []
    monkeypatch.setattr(prizes.secrets, "choice", lambda options: choices.append(options) or options[0])

    worker.grant(task)
    worker.grant(task)

    awards = [r for r in prizes.records(rig, "ABC123") if r.get("id", "").startswith("award_")]
    assert len(awards) == 2
    assert all(r["prize"]["id"] == "coffee" for r in awards)
    assert meta(rig)["stackPrizeStock"]["coffee"]["remaining"] == 0
    assert len([r for r in prizes.records(rig, "ABC123") if r.get("kind") == "allocation"]) == 1
    assert len(choices) == 0


def test_maximum_and_rule_fits_one_dynamodb_transaction(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    catalog = [{"id": f"prize-{i}", "source": "stand", "name": f"Prize {i}", "stockTotal": 10}
               for i in range(8)]
    program = {"version": 3, "storefrontUrl": "", "catalog": catalog, "rules": [{
        "id": "max", "condition": "completed",
        "groups": [{"allocations": [{"prizeId": p["id"], "quantity": 10} for p in catalog]}],
    }]}
    program["rules"][0]["groups"] *= 8
    game = meta(rig)
    game["stackPrizeProgram"] = program
    game["stackPrizeStock"] = {
        p["id"]: {"total": 10, "remaining": 10} for p in catalog
    }
    rig.Table("games").put_item(Item=game)
    task = prizes.job(game, "completed", [{"playerId": 1, "rank": 1}])
    task["program"] = program
    rig.Table("prizes").put_item(Item=task)

    worker.grant(task)

    assert len([r for r in prizes.records(rig, "ABC123") if r.get("id", "").startswith("award_")]) == 80
    assert len(rig.last_transaction) == 82
    assert len(rig.last_transaction) <= 100


def test_migration_counts_existing_awards_and_old_jobs_do_not_duplicate_them(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    program = prizes.upgrade_program(PROGRAM)
    old_awards = prizes.award_rows(META, PLAYER, PROGRAM, {"completed", "top_finish"}, rank=1)
    for award in old_awards:
        rig.Table("prizes").put_item(Item=award)
    game = meta(rig)
    game["stackPrizeStock"] = prizes.initial_stock(program, old_awards)
    rig.Table("games").put_item(Item=game)
    task = prizes.job(game, "finish", [{"playerId": 1, "rank": 1}])
    task["program"] = program
    rig.Table("prizes").put_item(Item=task)

    worker.grant(task)

    awards = [r for r in prizes.records(rig, "ABC123") if r.get("id", "").startswith("award_")]
    assert len(awards) == len(old_awards) == 4
    assert meta(rig)["stackPrizeStock"]["pizza"]["remaining"] == 998
    assert meta(rig)["stackPrizeStock"]["coffee"]["remaining"] == 998
    assert len([r for r in prizes.records(rig, "ABC123") if r.get("kind") == "allocation"]) == 2


def test_unconfirmed_earn_response_requires_reconciliation_and_is_not_replayed(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    row = next(r for r in prizes.award_rows(META, PLAYER, PROGRAM, {"completed"}, 1)
               if r["prize"]["source"] == "kivly")
    rig.Table("prizes").put_item(Item=row)
    client = MagicMock()
    client.customer_rewards.return_value = []
    client.earn_reward.return_value = {"success": False}
    monkeypatch.setattr(worker.registration, "connection", lambda *_: {"locationId": "loc", "ownerId": "owner"})
    monkeypatch.setattr(worker.registration, "_client", lambda *_: client)
    event = {"Records": [{"messageId": "message",
                          "body": json.dumps({"gameId": row["gameId"], "id": row["id"]})}]}

    assert worker.lambda_handler(event, None) == {"batchItemFailures": []}

    current = rig.Table("prizes").get_item(Key={"gameId": row["gameId"], "id": row["id"]})["Item"]
    assert current["state"] == "reconciliation_required"
    assert current["lastErrorStage"] == "earn"
    assert current["lastError"] == "KivlyEarnNotConfirmed"
    assert client.earn_reward.call_count == 1
    worker.deliver(current)
    assert client.earn_reward.call_count == 1


def test_read_only_delivery_mode_keeps_award_queued_without_kivly_requests(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    monkeypatch.setenv("KIVLY_EXTERNAL_WRITES_ENABLED", "false")
    row = next(r for r in prizes.award_rows(META, PLAYER, PROGRAM, {"completed"}, 1)
               if r["prize"]["source"] == "kivly")
    rig.Table("prizes").put_item(Item=row)
    client = MagicMock()
    client.customer_rewards.return_value = []
    monkeypatch.setattr(worker.registration, "connection", lambda *_: {"locationId": "loc", "ownerId": "owner"})
    monkeypatch.setattr(worker.registration, "_client", lambda *_: client)

    worker.deliver(row)

    current = rig.Table("prizes").get_item(Key={"gameId": row["gameId"], "id": row["id"]})["Item"]
    assert current["state"] == "queued" and current["work"] == "pending"
    assert current["dueAt"] > 0
    client.customer_rewards.assert_not_called()
    client.earn_reward.assert_not_called()


def test_wallet_uses_verified_membership_without_earning_a_reward(rig, monkeypatch):
    monkeypatch.setattr(prizes.registration, "connection", lambda *_: {"locationId": "loc"})
    result = prizes.public_awards(rig, META, PLAYER, lambda _: "unused")
    assert result["awards"] == []
    assert result["walletUrl"] == "https://example.kivly.store/add-to-wallet?customer_id=customer&location_id=loc"
    wrong = {**PLAYER, "kivlyMembership": {"locationId": "other", "customerId": "customer"}}
    assert prizes.wallet_link(rig, META, wrong) is None


@pytest.mark.parametrize("shared,expected", [
    ({}, "https://example.kivly.store"),
    ({"storefrontUrl": "https://shared.kivly.store"}, "https://shared.kivly.store"),
    ({"storefrontUrl": ""}, ""),
])
def test_wallet_and_program_reads_share_account_storefront_without_promoting_old_urls(rig, monkeypatch, shared, expected):
    from stand_common import stack_prize_api
    conn = {"locationId": "loc", "clubSettings": shared}
    monkeypatch.setattr(prizes.registration, "connection", lambda *_: conn)
    original = deepcopy(META)
    for game in (META, {**META, "stackPrizeProgram": {**PROGRAM, "storefrontUrl": "https://other.kivly.store"}}):
        response = stack_prize_api.response(rig, game)
        read = json.loads(response["body"])["program"]
        origin = expected if shared else game["stackPrizeProgram"]["storefrontUrl"]
        assert read["storefrontUrl"] == origin
        wallet = prizes.wallet_link(rig, game, PLAYER)
        assert wallet == (origin + "/add-to-wallet?customer_id=customer&location_id=loc" if origin else None)
    assert META == original
    assert conn["clubSettings"] == shared


def test_promotional_rules_union_groups_deduplicates_and_signs_only_committed_owned_images(monkeypatch):
    s3 = MagicMock()
    s3.generate_presigned_url.side_effect = lambda _, **kw: "signed:" + kw["Params"]["Key"]
    monkeypatch.setattr(prizes.boto3, "client", lambda _: s3)
    monkeypatch.setenv("USER_MEDIA_BUCKET", "media")
    key = "games/owner/ABC123/ranking/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.png"
    program = deepcopy(PROGRAM)
    program["catalog"][0]["imageKey"] = key
    program["catalog"][1]["imageUrl"] = "https://cdn.kivly.store/coffee.png"
    program["rules"] = [{"id": "promo", "condition": "top_final", "topX": 5, "groups": [
        {"allocations": [{"prizeId": "pizza", "quantity": 3}]},
        {"allocations": [{"prizeId": "pizza", "quantity": 1}, {"prizeId": "coffee", "quantity": 1}]},
    ]}]
    rules = prizes.promotional_rules({**META, "stackPrizeProgram": program})
    assert rules == [{"condition": "top_final", "topX": 5, "names": ["Pizza", "Coffee"], "prizes": [
        {"id": "pizza", "name": "Pizza", "imageUrl": "signed:" + key},
        {"id": "coffee", "name": "Coffee", "imageUrl": "https://cdn.kivly.store/coffee.png"},
    ]}]
    for unsafe in (key.replace("owner", "someone-else"), key.replace("ABC123", "XYZ789"),
                   "https://evil.test/image.png", "staging/" + key):
        assert prizes.image_url({**program["catalog"][0], "imageKey": unsafe}, META) is None
    assert s3.generate_presigned_url.call_count == 1


@pytest.mark.parametrize("old", [LEGACY_PROGRAM, v2_program(), PROGRAM])
def test_owner_and_public_metadata_expose_same_normalized_promotional_rules(rig, monkeypatch, old):
    from tests.test_stack_integration import game, operator_event
    from tests.test_public_web import public_mod
    from stand_common.stack import DEFAULT_CONFIG
    saved = {**meta(rig), "stackPrizeProgram": deepcopy(old), "stackPrizeState": "closed",
             "isActive": False, "type": {"STACK": deepcopy(DEFAULT_CONFIG)}}
    rig.Table("games").put_item(Item=saved)
    monkeypatch.setattr(prizes.registration, "connection", lambda *_: {"locationId": "loc",
                        "clubSettings": {"storefrontUrl": "https://shared.kivly.store"}})
    monkeypatch.setattr(game, "games_table", rig.Table("games"))
    monkeypatch.setattr(game, "dynamodb", rig)
    owner_result = game._handle_get(operator_event({}, gameId="ABC123"))
    assert owner_result["statusCode"] == 200
    owner = json.loads(owner_result["body"])
    assert owner["stackPrizeProgram"]["version"] == 3
    assert owner["stackPrizeProgram"]["storefrontUrl"] == "https://shared.kivly.store"
    assert owner["stackPrizeState"] == "closed" and owner["game"]["isActive"] is False
    monkeypatch.setattr(public_mod, "_get_game", lambda _: saved)
    monkeypatch.setattr(public_mod, "gp_table", rig.Table("players"))
    monkeypatch.setattr(public_mod, "_client_appearance", lambda _: {})
    monkeypatch.setattr(public_mod.kivly_registration, "game_settings", lambda _: {"enabled": False})
    public_result = public_mod._handle_get_game({"queryStringParameters": {"gameId": "ABC123"}})
    assert public_result["statusCode"] == 200
    public = json.loads(public_result["body"])
    assert owner["stackPrizeRules"] == public["stackPrizeRules"]
    assert owner["stackPrizeRules"][0]["prizes"][0] == {"id": "pizza", "name": "Pizza", "imageUrl": None}


def test_close_keeps_active_until_pending_finish_then_atomically_deactivates(rig):
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    with pytest.raises(prizes.PrizeError, match="StackClosingStarted"):
        prizes.close(rig, meta(rig), [])
    assert meta(rig)["isActive"] is True
    prizes.close(rig, meta(rig), [])
    assert meta(rig)["stackPrizeState"] == "closing" and meta(rig)["isActive"] is True
    finish(rig)
    prizes.close(rig, meta(rig), [])
    assert meta(rig)["stackPrizeState"] == "closed" and meta(rig)["isActive"] is False
    assert len([r for r in prizes.records(rig, "ABC123") if r.get("condition") == "top_final"]) == 1
    prizes.close(rig, meta(rig), [])
    assert len([r for r in prizes.records(rig, "ABC123") if r.get("condition") == "top_final"]) == 1


def test_close_api_supports_stack_without_saved_program(rig, monkeypatch):
    from stand_common import stack_prize_api
    monkeypatch.setattr(prizes.registration, "connection", lambda *_: None)
    saved = meta(rig)
    saved.pop("stackPrizeProgram")
    rig.Table("games").put_item(Item=saved)
    event = {"requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
             "body": json.dumps({"gameId": "ABC123"})}
    first = stack_prize_api.handle(event, "POST", rig, lambda _: True, lambda _: b"")
    assert first["statusCode"] == 200
    assert json.loads(first["body"])["state"] == "closing"
    assert meta(rig)["isActive"] is True and meta(rig)["stackPrizeProgram"]["version"] == 3
    final = stack_prize_api.handle(event, "POST", rig, lambda _: True, lambda _: b"")
    assert final["statusCode"] == 200
    assert json.loads(final["body"])["state"] == "closed" and meta(rig)["isActive"] is False


def test_generic_stack_deactivation_requires_dedicated_close(rig, monkeypatch):
    from tests.test_stack_integration import game, operator_event
    monkeypatch.setattr(game, "games_table", rig.Table("games"))
    response = game._handle_put(operator_event({"gameId": "ABC123", "isActive": False}))
    assert response["statusCode"] == 409
    assert json.loads(response["body"])["error"] == "StackCloseRequired"
    assert meta(rig)["isActive"] is True


def test_real_sdk_serializes_native_transaction_values_once(rig):
    from tests.test_game_players import _sdk_transaction_payload
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    finish(rig)
    wire = _sdk_transaction_payload(rig.last_transaction)
    assert wire[0]["Update"]["Key"]["gameId"] == {"S": "ABC123"}
    assert wire[1]["Update"]["ExpressionAttributeValues"][":score"] == {"N": "0"}
    assert wire[2]["Put"]["Item"]["condition"] == {"S": "finish"}
    assert wire[2]["Put"]["Item"]["program"]["M"]["version"] == {"N": "3"}


def test_top_finish_award_survives_a_later_better_score(rig, monkeypatch):
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    finish(rig, 0)
    process_grants(rig, monkeypatch)
    another = deepcopy(PLAYER)
    another["playerId"] = 2
    another["type"]["STACK"] = {"score": 99, "attempt": {
        **ATTEMPT, "status": "finished", "finishedAt": "2026-10-26T12:02:00Z",
    }}
    rig.Table("players").put_item(Item=another)
    assert len([r for r in prizes.records(rig, "ABC123")
                if r.get("condition") == "top_finish"]) == 2


def test_final_prizes_are_delivered_by_worker_without_a_player_request(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    finish(rig)
    with pytest.raises(prizes.PrizeError):
        prizes.close(rig, meta(rig), [])
    worker.finish_close({"gameId": "ABC123", "id": "close"})
    task = next(r for r in prizes.records(rig, "ABC123")
                if r.get("kind") == "grant" and r.get("condition") == "top_final")
    worker.grant(task)
    assert any(r.get("condition") == "top_final" and r.get("work") == "pending"
               for r in prizes.records(rig, "ABC123"))


def test_finish_after_new_close_revision_rolls_back(rig):
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    stale_meta, stale_player = meta(rig), player(rig)
    with pytest.raises(prizes.PrizeError):
        prizes.close(rig, meta(rig), [])
    with pytest.raises(prizes.PrizeError, match="StackPrizeConflict"):
        prizes.finish(rig, stale_meta, stale_player,
                      {**ATTEMPT, "status": "finished", "finishedAt": "2026-10-26T12:01:00Z"}, 1)
    assert "score" not in player(rig)["type"]["STACK"]
    assert finish(rig) == 1


def test_owner_api_rejects_other_owners_and_non_stack_games(rig):
    from stand_common.stack_prize_api import handle
    event = {"requestContext": {"authorizer": {"jwt": {"claims": {"sub": "other"}}}},
             "queryStringParameters": {"gameId": "ABC123"}}
    result = handle(event, "GET", rig, lambda _: True, lambda _: b"")
    assert result["statusCode"] == 403
    event["requestContext"]["authorizer"]["jwt"]["claims"]["sub"] = "owner"
    game = meta(rig)
    game["gameType"] = "T1MER"
    rig.Table("games").put_item(Item=game)
    result = handle(event, "GET", rig, lambda _: True, lambda _: b"")
    assert result["statusCode"] == 400


def test_owner_api_migrates_legacy_program_and_initializes_shared_stock(rig, monkeypatch):
    from stand_common import stack_prize_api
    monkeypatch.setattr(stack_prize_api.registration, "connection", lambda *_: None)
    legacy = {"version": 1, "storefrontUrl": "", "catalog": [
        {"id": "pizza", "source": "stand", "name": "Pizza", "limit": None},
    ], "rules": [{"id": "new_done", "condition": "completed", "prizeIds": ["pizza"]}]}
    old_award = prizes.award_rows(META, PLAYER, legacy, {"completed"}, 1)[0]
    rig.Table("prizes").put_item(Item=old_award)
    event = {"requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
             "body": json.dumps({"gameId": "ABC123", "program": legacy})}

    result = stack_prize_api.handle(event, "PUT", rig, lambda _: True, lambda _: b"")

    assert result["statusCode"] == 200, result
    saved = meta(rig)
    assert saved["stackPrizeProgram"]["version"] == 3
    assert saved["stackPrizeProgram"]["rules"][0]["groups"][0]["allocations"][0]["quantity"] == 1
    assert saved["stackPrizeStock"]["pizza"] == {"total": 1000, "remaining": 999}


def test_legacy_adapter_preserves_limits_images_and_rule_conditions():
    legacy = {"prizeCatalog": {"ranking": {"name": "Pizza", "topX": 10, "limit": 3, "imageKey": None}},
              "kivlyReward": {"id": "r1", "name": "Coffee", "locationId": "loc"}}
    program = prizes.legacy_program(legacy)
    assert program["catalog"][0]["limit"] == 3
    assert program["rules"][0]["topX"] == 10
    assert program["rules"][1]["condition"] == "completed"
    assert len(prizes.award_rows(META, PLAYER, program, {"top_finish"}, rank=4)) == 0


def test_managed_score_handler_start_finish_and_retry_preserve_one_set_of_awards(rig, monkeypatch):
    from tests.test_stack_score import stack, DEFAULT_CONFIG, HISTORY
    game = meta(rig)
    game["type"] = {"STACK": deepcopy(DEFAULT_CONFIG)}
    rig.Table("games").put_item(Item=game)
    monkeypatch.setattr(stack.boto3, "resource", lambda _: rig)
    timestamps = iter(["2026-10-26T12:00:00Z", "2026-10-26T12:01:00Z"])
    monkeypatch.setattr(stack, "_iso_now", lambda: next(timestamps))
    payload = {"gameId": "ABC123", "playerId": 1, "attemptId": "real-attempt", "action": "start"}
    started = stack.store_score("ABC123", rig.Table("players"), payload, game)
    assert started["ok"] and started["status"] == "started"
    payload.update(action="finish", placements=HISTORY)
    result = stack.store_score("ABC123", rig.Table("players"), payload, game)
    assert result["ok"] and result["score"] == 1 and result["rank"] == 1
    assert stack.store_score("ABC123", rig.Table("players"), payload, game) == result
    process_grants(rig, monkeypatch)
    assert len([r for r in prizes.records(rig, "ABC123") if r["id"].startswith("award_")]) == 4


def test_worker_defers_pre_earn_failures_with_visible_error_and_recovers(rig, monkeypatch):
    worker = delivery_module(monkeypatch, rig)
    row = next(r for r in prizes.award_rows(META, PLAYER, PROGRAM, {"completed"}, 1) if r["prize"]["source"] == "kivly")
    rig.Table("prizes").put_item(Item=row)
    monkeypatch.setattr(worker.registration, "connection", lambda *_: None)
    event = {"Records": [{"messageId": "message", "body": json.dumps({"gameId": row["gameId"], "id": row["id"]})}]}
    assert worker.lambda_handler(event, None) == {"batchItemFailures": []}
    current = rig.Table("prizes").get_item(Key={"gameId": row["gameId"], "id": row["id"]})["Item"]
    assert current["state"] == "queued" and current["work"] == "pending"
    assert current["lastError"] == "KivlyConnectionChanged" and current["dueAt"] > 0
    client = MagicMock()
    client.customer_rewards.side_effect = [[], [{"id": "new", "reward_id": "r1"}]]
    client.earn_reward.return_value = {"success": True}
    monkeypatch.setattr(worker.registration, "connection", lambda *_: {"locationId": "loc", "ownerId": "owner"})
    monkeypatch.setattr(worker.registration, "_client", lambda *_: client)
    assert worker.lambda_handler(event, None) == {"batchItemFailures": []}
    recovered = rig.Table("prizes").get_item(Key={"gameId": row["gameId"], "id": row["id"]})["Item"]
    assert recovered["state"] == "confirmed" and "lastError" not in recovered
    client.earn_reward.assert_called_once()


@pytest.mark.parametrize("catalog", [None, {}, "invalid"])
def test_owner_api_rejects_malformed_program_without_internal_error(rig, monkeypatch, catalog):
    from stand_common import stack_prize_api
    monkeypatch.setattr(stack_prize_api.registration, "connection", lambda *_: None)
    event = {"requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
             "body": json.dumps({"gameId": "ABC123", "program": {**PROGRAM, "catalog": catalog}})}
    result = stack_prize_api.handle(event, "PUT", rig, lambda _: True, lambda _: b"")
    assert result["statusCode"] == 400


def test_closed_paused_game_metadata_and_earned_prizes_remain_accessible(rig, monkeypatch):
    from stand_common.stack import DEFAULT_CONFIG
    from tests.test_public_web import public_mod
    prizes.start(rig, meta(rig), player(rig), ATTEMPT)
    finish(rig)
    process_grants(rig, monkeypatch)
    game = {**meta(rig), "isActive": False, "stackPrizeState": "closed",
            "type": {"STACK": deepcopy(DEFAULT_CONFIG)}}
    monkeypatch.setattr(public_mod, "dynamo_r", rig)
    monkeypatch.setattr(public_mod, "gp_table", rig.Table("players"))
    monkeypatch.setattr(public_mod, "_get_game", lambda _: game)
    monkeypatch.setattr(public_mod, "_client_appearance", lambda _: {})
    monkeypatch.setattr(public_mod.kivly_registration, "game_settings", lambda _: {"enabled": False})
    monkeypatch.setattr(public_mod.kivly_registration, "connection", lambda *_: {"locationId": "loc"})
    response = public_mod._handle_get_game({"queryStringParameters": {"gameId": "ABC123"}})
    assert response["statusCode"] == 200
    assert json.loads(response["body"])["stackPrizeState"] == "closed"
    response = public_mod._player_response(game, player(rig), "ABC123", 1, "player-token")
    assert response["statusCode"] == 200
    result = json.loads(response["body"])
    assert len(result["stackPrizes"]["awards"]) == 4
    assert result["prizes"] == []


def test_raffle_persists_winners_and_award_jobs_without_duplicates(rig, monkeypatch):
    path = Path(__file__).parents[1] / "src" / "stand_prod_game_fn_raffle" / "lambda_function.py"
    spec = importlib.util.spec_from_file_location("stack_raffle_tests", path)
    raffle = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(raffle)
    monkeypatch.setattr(raffle, "dynamo_r", rig)
    monkeypatch.setattr(raffle, "games_table", rig.Table("games"))
    excluded = deepcopy(PLAYER)
    excluded["playerId"] = 2
    excluded["type"]["STACK"]["attempt"] = {**ATTEMPT, "status": "excluded"}
    rig.Table("players").put_item(Item=excluded)
    event = {"requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
             "body": json.dumps({"gameId": "ABC123", "numberOfWinners": 5})}
    assert raffle.lambda_handler(event, None)["statusCode"] == 200
    assert meta(rig)["raffleWinners"][0]["playerId"] == 1
    assert len(meta(rig)["raffleWinners"]) == 1
    assert raffle.lambda_handler(event, None)["statusCode"] == 200
    tasks = [r for r in prizes.records(rig, "ABC123") if r.get("kind") == "grant"]
    assert len(tasks) == 1 and tasks[0]["winners"] == [{"playerId": 1, "rank": 1}]
    worker = delivery_module(monkeypatch, rig)
    worker.grant(tasks[0])
    worker.grant(tasks[0])
    assert len([r for r in prizes.records(rig, "ABC123") if r["id"].startswith("award_")]) == 1
