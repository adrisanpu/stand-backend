import json
from copy import deepcopy
from decimal import Decimal

import pytest
from stand_common import reward_conditions, stack_prize_api, stack_prizes as prizes
from tests.test_stack_prize_rules import rig, meta  # noqa: F401
from tests.test_stack_integration import game, operator_event
from stand_common.stack import DEFAULT_CONFIG
from stand_common.utils import _json_sanitize


@pytest.fixture
def configured(rig, monkeypatch):
    monkeypatch.setattr(prizes.registration, "connection", lambda *_: None)
    saved = meta(rig)
    saved["gameName"] = "Example"
    saved["type"] = {"STACK": deepcopy(DEFAULT_CONFIG)}
    saved["stackPrizeProgram"] = {
        "version": 3, "storefrontUrl": "", "deliveryChannel": "wallet",
        "catalog": [{"id": "pizza", "source": "stand", "name": "Pizza",
                     "stockTotal": 100, "limit": None, "imageKey": None}],
        "rules": [{"id": "done", "condition": "completed",
                   "groups": [{"allocations": [{"prizeId": "pizza", "quantity": 1}]}]}],
    }
    saved["stackPrizeStock"] = {"pizza": {"total": 100, "remaining": 97}}
    rig.Table("games").put_item(Item=saved)
    monkeypatch.setattr(game, "games_table", rig.Table("games"))
    monkeypatch.setattr(game, "dynamodb", rig)
    return rig


def request(dynamo, body, method="PUT", owner="owner"):
    event = operator_event({"gameId": "ABC123", **body})
    event["requestContext"]["authorizer"]["jwt"]["claims"]["sub"] = owner
    if method == "GET":
        event["queryStringParameters"] = {"gameId": "ABC123"}
    return stack_prize_api.handle(event, method, dynamo, lambda _: True, lambda _: b"")


def test_revision_is_stable_for_dynamo_numbers():
    assert reward_conditions.revision({"n": Decimal("3"), "v": [1]}) == reward_conditions.revision({"v": [1], "n": 3})


def test_partial_rules_preserve_catalog_stock_channel_players_and_awards(configured):
    before = meta(configured)
    old_program = before["stackPrizeProgram"]
    response = request(configured, {}, "GET")
    revision = json.loads(response["body"])["programRevision"]
    changed = deepcopy(old_program["rules"])
    changed[0].update(id="new-rule", condition="top_final", topX=2)
    configured.Table("prizes").put_item(Item={"gameId": "ABC123", "id": "award_example", "state": "available"})
    result = request(configured, {"action": "rules", "expectedRevision": revision, "rules": changed})
    assert result["statusCode"] == 200, result
    after = meta(configured)
    assert after["stackPrizeProgram"] == {**old_program, "rules": changed}
    assert after["stackPrizeStock"] == before["stackPrizeStock"]
    assert configured.Table("prizes").get_item(Key={"gameId": "ABC123", "id": "award_example"})["Item"]["state"] == "available"
    assert configured.Table("players").get_item(Key={"gameId": "ABC123", "playerId": 1})["Item"]["username"] == "Player"
    assert json.loads(result["body"])["programRevision"] != revision


@pytest.mark.parametrize("consumed", [0, 3])
def test_removing_last_rule_releases_only_unconsumed_reservation(configured, consumed):
    saved = meta(configured)
    saved["stackPrizeStock"]["pizza"]["remaining"] = 100 - consumed
    configured.Table("games").put_item(Item=saved)
    award = {"gameId": "ABC123", "id": "award_existing", "state": "available"}
    configured.Table("prizes").put_item(Item=award)
    result = request(configured, {
        "action": "rules", "rules": [],
        "expectedRevision": reward_conditions.revision(saved["stackPrizeProgram"]),
    })
    assert result["statusCode"] == 200, result
    assert meta(configured)["stackPrizeProgram"]["catalog"][0]["stockTotal"] == consumed
    assert meta(configured)["stackPrizeStock"]["pizza"] == {"total": consumed, "remaining": 0}
    assert configured.Table("prizes").get_item(Key={"gameId": "ABC123", "id": "award_existing"})["Item"] == award
    assert json.loads(result["body"])["program"]["catalog"][0]["stockTotal"] == consumed


def test_removing_one_rule_keeps_reservation_when_another_rule_uses_prize(configured):
    saved = meta(configured)
    saved["stackPrizeProgram"]["rules"].append({
        **deepcopy(saved["stackPrizeProgram"]["rules"][0]), "id": "another", "condition": "raffle",
    })
    configured.Table("games").put_item(Item=saved)
    result = request(configured, {
        "action": "rules", "rules": saved["stackPrizeProgram"]["rules"][1:],
        "expectedRevision": reward_conditions.revision(saved["stackPrizeProgram"]),
    })
    assert result["statusCode"] == 200, result
    assert meta(configured)["stackPrizeProgram"]["catalog"][0]["stockTotal"] == 100
    assert meta(configured)["stackPrizeStock"] == saved["stackPrizeStock"]


def test_saving_rules_also_reconciles_previously_orphaned_reservations(configured):
    saved = meta(configured)
    saved["stackPrizeProgram"]["rules"] = []
    configured.Table("games").put_item(Item=saved)
    result = request(configured, {
        "action": "rules", "rules": [],
        "expectedRevision": reward_conditions.revision(saved["stackPrizeProgram"]),
    })
    assert result["statusCode"] == 200, result
    assert meta(configured)["stackPrizeProgram"]["catalog"][0]["stockTotal"] == 3
    assert meta(configured)["stackPrizeStock"]["pizza"] == {"total": 3, "remaining": 0}


def test_removing_kivly_prize_releases_its_reservation_without_touching_awards(configured, monkeypatch):
    saved = meta(configured)
    saved["stackPrizeProgram"]["catalog"][0] = {
        "id": "pizza", "source": "kivly", "name": "Pizza", "stockTotal": 100,
        "locationId": "club", "rewardId": "reward",
    }
    configured.Table("games").put_item(Item=saved)
    conn = {"locationId": "club", "ownerId": "external-owner"}
    monkeypatch.setattr(prizes.registration, "connection", lambda *_: conn)
    monkeypatch.setattr(prizes.registration, "account_game_settings", lambda *_: {"enabled": True})
    from unittest.mock import MagicMock
    client = MagicMock()
    client.list_rewards.return_value = [{"id": "reward", "name": "Pizza"}]
    monkeypatch.setattr(prizes.registration, "_client", lambda *_: client)
    award = {"gameId": "ABC123", "id": "award_kivly", "state": "confirmed",
             "prize": deepcopy(saved["stackPrizeProgram"]["catalog"][0])}
    configured.Table("prizes").put_item(Item=award)
    result = request(configured, {
        "action": "rules", "rules": [],
        "expectedRevision": reward_conditions.revision(saved["stackPrizeProgram"]),
    })
    assert result["statusCode"] == 200, result
    assert meta(configured)["stackPrizeProgram"]["catalog"][0]["stockTotal"] == 3
    assert meta(configured)["stackPrizeStock"]["pizza"] == {"total": 3, "remaining": 0}
    assert configured.Table("prizes").get_item(Key={"gameId": "ABC123", "id": "award_kivly"})["Item"] == award


@pytest.mark.parametrize("extra,expected", [
    ({"expectedRevision": "outdated"}, 409),
    ({"program": {}}, 400),
    ({"catalog": []}, 400),
])
def test_partial_rules_reject_stale_or_extra_fields(configured, extra, expected):
    before = meta(configured)
    result = request(configured, {
        "action": "rules", "expectedRevision": reward_conditions.revision(before["stackPrizeProgram"]),
        "rules": before["stackPrizeProgram"]["rules"], **extra,
    })
    assert result["statusCode"] == expected
    assert meta(configured) == before


@pytest.mark.parametrize("updates", [
    {"isActive": False}, {"isActive": False, "stackPrizeState": "closing"}, {"archivedAt": "2026-11-04"},
])
def test_partial_rules_reject_inactive_closing_and_archived(configured, updates):
    saved = {**meta(configured), **updates}
    configured.Table("games").put_item(Item=saved)
    result = request(configured, {"action": "rules", "expectedRevision": reward_conditions.revision(saved["stackPrizeProgram"]),
                                  "rules": []})
    assert result["statusCode"] == 409
    assert meta(configured) == saved


def test_partial_rules_are_owner_only(configured):
    assert request(configured, {"action": "rules", "rules": [], "expectedRevision": "x"}, owner="other")["statusCode"] == 403


def test_catalog_edit_ignores_stale_rules(configured):
    saved = meta(configured)
    saved["stackPrizeProgram"]["rules"][0]["id"] = "latest"
    configured.Table("games").put_item(Item=saved)
    stale = deepcopy(saved["stackPrizeProgram"])
    stale["rules"] = []
    stale["catalog"][0]["name"] = "Updated pizza"
    result = request(configured, {"action": "catalog", "program": stale})
    assert result["statusCode"] == 200, result
    assert meta(configured)["stackPrizeProgram"]["rules"] == saved["stackPrizeProgram"]["rules"]
    assert meta(configured)["stackPrizeProgram"]["catalog"][0]["name"] == "Updated pizza"
    assert meta(configured)["stackPrizeStock"]["pizza"]["remaining"] == 97


def test_rules_reject_deactivation_between_read_and_transaction(configured, monkeypatch):
    saved = meta(configured)
    original = prizes.transact

    def close_then_write(dynamo, operations):
        dynamo.Table("games").put_item(Item={**saved, "isActive": False})
        original(dynamo, operations)

    monkeypatch.setattr(prizes, "transact", close_then_write)
    result = request(configured, {"action": "rules", "rules": [],
                                  "expectedRevision": reward_conditions.revision(saved["stackPrizeProgram"])})
    assert result["statusCode"] == 409
    assert meta(configured)["stackPrizeProgram"] == saved["stackPrizeProgram"]


def native_meta(dynamo):
    saved = {**meta(dynamo), "gameType": "T1MER", "prizeCatalog": {
        "ranking": {"name": "Medal", "topX": 3, "limit": 10, "imageKey": None},
        "raffle": {"name": "Draw", "limit": None, "imageKey": None},
    }}
    dynamo.Table("games").put_item(Item=saved)
    return saved


def test_native_top_only_preserves_other_prize_fields(configured):
    saved = native_meta(configured)
    result = game._handle_put(operator_event({"gameId": "ABC123", "rewardConditions": {
        "rankingTopX": 7, "expectedRevision": reward_conditions.revision(saved["prizeCatalog"]),
    }}))
    assert result["statusCode"] == 200, result
    assert meta(configured)["prizeCatalog"] == {
        **saved["prizeCatalog"], "ranking": {**saved["prizeCatalog"]["ranking"], "topX": 7},
    }
    assert json.loads(result["body"])["rewardConditionsRevision"] == reward_conditions.revision(meta(configured)["prizeCatalog"])


@pytest.mark.parametrize("value", [0, 101, True, 1.5, "3"])
def test_native_top_rejects_invalid_values(configured, value):
    saved = native_meta(configured)
    with pytest.raises(prizes.PrizeError, match="InvalidRewardConditions"):
        reward_conditions.native_update(configured, saved, {
            "rankingTopX": value, "expectedRevision": reward_conditions.revision(saved["prizeCatalog"]),
        })


def test_native_top_rejects_catalog_race_without_revision_increment(configured):
    saved = native_meta(configured)
    latest = deepcopy(saved)
    latest["prizeCatalog"]["ranking"]["name"] = "New name"
    configured.Table("games").put_item(Item=latest)
    with pytest.raises(prizes.PrizeError, match="StackPrizeConflict"):
        reward_conditions.native_update(configured, saved, {
            "rankingTopX": 7, "expectedRevision": reward_conditions.revision(saved["prizeCatalog"]),
        })
    assert meta(configured)["prizeCatalog"] == latest["prizeCatalog"]


def test_native_catalog_save_preserves_current_ranking_top(configured, monkeypatch):
    saved = native_meta(configured)
    monkeypatch.setattr(game, "_get_game", lambda _: meta(configured))
    monkeypatch.setattr(game, "_get_user", lambda _: {})
    monkeypatch.setattr(game, "_is_user_pro", lambda _: True)
    catalog = deepcopy(saved["prizeCatalog"])
    catalog["ranking"].update(topX=1, name="New medal")
    result = game._handle_put(operator_event({"gameId": "ABC123", "prizeCatalog": catalog,
                                             "preserveRewardConditions": True}))
    assert result["statusCode"] == 200, result
    assert meta(configured)["prizeCatalog"]["ranking"]["topX"] == 3
    assert meta(configured)["prizeCatalog"]["ranking"]["name"] == "New medal"


@pytest.mark.parametrize("game_type", ["STACK", "T1MER"])
def test_legacy_implicit_active_events_can_edit_conditions(configured, game_type):
    saved = native_meta(configured) if game_type == "T1MER" else meta(configured)
    saved.pop("isActive")
    configured.Table("games").put_item(Item=saved)
    if game_type == "T1MER":
        reward_conditions.native_update(configured, saved, {
            "rankingTopX": 5, "expectedRevision": reward_conditions.revision(saved["prizeCatalog"]),
        })
        assert meta(configured)["prizeCatalog"]["ranking"]["topX"] == 5
    else:
        result = request(configured, {"action": "rules", "rules": [],
                                      "expectedRevision": reward_conditions.revision(saved["stackPrizeProgram"])})
        assert result["statusCode"] == 200, result


@pytest.mark.parametrize("active", [True, False])
def test_owned_configuration_snapshot_supports_legacy_inactive_games(configured, active):
    saved = {**meta(configured), "isActive": active}
    configured.Table("games").put_item(Item=saved)
    result = game._handle_get(operator_event({}, gameId="ABC123", configuration="true"))
    assert result["statusCode"] == 200, result
    body = json.loads(result["body"])
    assert body["game"]["gameId"] == "ABC123"
    assert body["game"]["isActive"] is active
    assert body["configuration"] == _json_sanitize(DEFAULT_CONFIG)
    assert body["rewardConditionsRevision"]
