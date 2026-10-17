"""STACK ranking claims use server ranks and one-time delivery."""
import json
from copy import deepcopy
from unittest.mock import MagicMock, patch

import pytest

from stand_common.utils import issue_prize_token, verify_prize_token
from tests.test_public_web import SECRET, public_mod, _redeem_event


META = {
    "gameId": "ABC123", "gameType": "STACK", "isActive": True,
    "prizeCatalog": {"ranking": {"name": "Consola", "limit": 3, "topX": 5}},
}
PLAYER = {
    "gameId": "ABC123", "playerId": 1, "username": "Winner",
    "type": {"STACK": {"score": 7, "attempt": {
        "attemptId": "attempt", "status": "finished", "startedAt": "2026-10-17T10:00:00Z",
        "finishedAt": "2026-10-17T10:01:00Z", "configVersion": "1",
    }}},
}


class ClientError(Exception):
    def __init__(self, response, operation):
        super().__init__(operation)
        self.response = response


@pytest.fixture(autouse=True)
def client_error_type(monkeypatch):
    monkeypatch.setattr(public_mod, "ClientError", ClientError)


@pytest.mark.parametrize("rank,prize,eligible", [
    (1, {"name": "Prize", "topX": 3, "limit": 3}, True),
    (3, {"name": "Prize", "topX": 3, "limit": None}, True),
    (4, {"name": "Prize", "topX": 3, "limit": None}, False),
    (3, {"name": "Prize", "topX": 5, "limit": 2}, False),
    (1, {"name": "Prize", "topX": 5, "limit": 0}, False),
    (None, {"name": "Prize", "topX": 3, "limit": 3}, False),
    (1, {"name": "Prize", "limit": 3}, False),
    (1, {"name": "", "topX": 3, "limit": 3}, False),
    (1, {"name": "", "imageKey": "image.png", "topX": 3, "limit": 3}, True),
    (1, {}, False),
])
def test_eligibility_requires_rank_within_top_and_available_prize(rank, prize, eligible):
    assert public_mod._stack_ranking_prize_eligible(
        {"prizeCatalog": {"ranking": prize}}, rank,
    ) is eligible


def test_get_player_issues_ranking_token_from_confirmed_rank():
    table = MagicMock()
    table.query.return_value = {"Items": [PLAYER]}
    with patch.object(public_mod, "gp_table", table):
        response = public_mod._player_response(META, PLAYER, "ABC123", 1, "session")
    payload = json.loads(response["body"])
    assert payload["assignment"]["rank"] == 1
    prize = payload["prizes"][0]
    assert prize["kind"] == "ranking"
    assert prize["delivered"] is False
    assert verify_prize_token(prize["token"], SECRET) == {
        "gameId": "ABC123", "playerId": 1, "kind": "k",
    }


def test_unfinished_player_does_not_receive_claim():
    player = deepcopy(PLAYER)
    player["type"]["STACK"] = {"attempt": {**PLAYER["type"]["STACK"]["attempt"], "status": "started"}}
    with patch.object(public_mod, "gp_table", MagicMock()):
        response = public_mod._player_response(META, player, "ABC123", 1, "session")
    assert json.loads(response["body"])["prizes"] == []


def test_delivered_claim_is_retained_even_after_rank_changes():
    player = deepcopy(PLAYER)
    player["type"]["STACK"]["rankingPrizeDelivered"] = True
    prizes = public_mod._stack_prizes(META, player, 10)
    assert prizes[0]["kind"] == "ranking"
    assert prizes[0]["delivered"] is True


def redeem(player=PLAYER, meta=META, rank=1, kind="k", table=None):
    table = table if table is not None else MagicMock()
    with patch.object(public_mod, "_get_game", return_value=meta), \
         patch.object(public_mod, "_get_player", return_value=player), \
         patch.object(public_mod, "gp_table", table), \
         patch.object(public_mod, "stack_ranking", return_value={
             "results": [{"playerId": 1, "rank": rank}] if rank is not None else [],
         }):
        response = public_mod._handle_redeem(_redeem_event(
            issue_prize_token("ABC123", 1, kind, SECRET),
        ))
    return response, table


def test_validator_redeems_ranking_prize_atomically():
    response, table = redeem()
    assert response["statusCode"] == 200
    assert json.loads(response["body"])["kind"] == "ranking"
    table.update_item.assert_called_once()
    write = table.update_item.call_args.kwargs
    assert write["ExpressionAttributeNames"]["#delivered"] == "rankingPrizeDelivered"
    assert "attribute_not_exists(#type.#g.#delivered)" in write["ConditionExpression"]
    assert "#type.#g.#attempt.#status = :finished" in write["ConditionExpression"]


def test_validator_returns_redeemed_prize_with_fresh_image():
    prize = {"name": "Consola", "limit": 3, "topX": 5, "imageUrl": "https://media.example/prize"}
    with patch.object(public_mod, "_public_prize_catalog", return_value={"ranking": prize}) as catalog:
        response, _ = redeem()
    assert response["statusCode"] == 200
    assert json.loads(response["body"])["prize"] == prize
    catalog.assert_called_once_with(META)


@pytest.mark.parametrize("rank,kind", [(6, "k"), (4, "k"), (None, "k"), (1, "v"), (1, "r")])
def test_validator_rechecks_eligibility_not_just_signed_token(rank, kind):
    response, table = redeem(rank=rank, kind=kind)
    assert response["statusCode"] == 400
    assert json.loads(response["body"])["error"] == "NotEligible"
    table.update_item.assert_not_called()


def test_removed_or_disabled_prize_cannot_be_redeemed():
    for prize in ({}, {"name": "Prize", "topX": 3, "limit": 0}):
        response, table = redeem(meta={**META, "prizeCatalog": {"ranking": prize}})
        assert response["statusCode"] == 400
        table.update_item.assert_not_called()


def test_duplicate_and_concurrent_redemption_are_rejected():
    player = deepcopy(PLAYER)
    player["type"]["STACK"]["rankingPrizeDelivered"] = True
    response, table = redeem(player=player)
    assert response["statusCode"] == 409
    table.update_item.assert_not_called()
    table.update_item.side_effect = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem",
    )
    response, _ = redeem(table=table)
    assert response["statusCode"] == 409


def test_database_failure_is_not_reported_as_success():
    table = MagicMock()
    table.update_item.side_effect = ClientError(
        {"Error": {"Code": "ProvisionedThroughputExceededException"}}, "UpdateItem",
    )
    response, _ = redeem(table=table)
    assert response["statusCode"] == 500
    assert json.loads(response["body"])["error"] == "UpdateFailed"


def test_ranking_token_cannot_redeem_other_prize_kinds():
    response, table = redeem(meta={**META, "gameType": "EMPAREJA2"})
    assert response["statusCode"] == 400
    table.update_item.assert_not_called()
