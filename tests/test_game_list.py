"""The administrative list includes every page of the owner's events."""
from unittest.mock import MagicMock, patch
from decimal import Decimal

from tests.email_fake import FakeDynamoError
from tests.test_game_players import event, game, response_body


def test_list_owned_games_reads_all_pages_and_sorts_them():
    table = MagicMock()
    table.query.side_effect = [
        {
            "Items": [{"gameId": "OLD", "gameName": "OLD", "gameType": "STACK",
                       "createdAt": "2026-01-01", "ownerUserId": "owner"}],
            "LastEvaluatedKey": {"gameId": "OLD", "ownerUserId": "owner"},
        },
        {"Items": [{"gameId": "NEW", "gameName": "NEW", "gameType": "T1MER",
                    "createdAt": "2026-02-01", "ownerUserId": "owner", "playersCount": Decimal(17), "isActive": False}]},
    ]
    with patch.object(game, "games_table", table):
        response = game.lambda_handler(event("GET", "/v1/game"), None)
    assert response["statusCode"] == 200
    body = response_body(response)
    assert body["count"] == 2
    assert [row["gameId"] for row in body["games"]] == ["NEW", "OLD"]
    assert body["games"][0]["createdAt"] == "2026-02-01"
    assert [row["playerCount"] for row in body["games"]] == [17, 0]
    assert [row["isActive"] for row in body["games"]] == [False, True]
    assert body["games"][1]["stackPrizeState"] == "open"
    assert table.query.call_args_list[0].kwargs["IndexName"] == game.GSI_OWNER
    assert "ExclusiveStartKey" not in table.query.call_args_list[0].kwargs
    assert table.query.call_args_list[1].kwargs["ExclusiveStartKey"] == {
        "gameId": "OLD", "ownerUserId": "owner",
    }


def test_type_filter_applies_to_later_pages_too():
    table = MagicMock()
    table.query.side_effect = [
        {"Items": [{"gameId": "A", "gameType": "STACK"}], "LastEvaluatedKey": {"gameId": "A"}},
        {"Items": [{"gameId": "B", "gameType": "T1MER"}]},
    ]
    with patch.object(game, "games_table", table):
        response = game.lambda_handler(event("GET", "/v1/game", query={"gameType": "T1MER"}), None)
    assert response["statusCode"] == 200
    assert [row["gameId"] for row in response_body(response)["games"]] == ["B"]


def test_later_page_failure_is_an_error_not_a_partial_list():
    table = MagicMock()
    table.query.side_effect = [
        {"Items": [{"gameId": "A"}], "LastEvaluatedKey": {"gameId": "A"}},
        FakeDynamoError({"Error": {"Code": "InternalServerError"}}, "Query"),
    ]
    with patch.object(game, "games_table", table), patch.object(game, "ClientError", FakeDynamoError):
        response = game.lambda_handler(event("GET", "/v1/game"), None)
    assert response["statusCode"] == 500
    assert "games" not in response_body(response)


def test_list_requires_authenticated_owner():
    table = MagicMock()
    with patch.object(game, "games_table", table):
        response = game.lambda_handler(event("GET", "/v1/game", owner=""), None)
    assert response["statusCode"] == 401
    table.query.assert_not_called()
