"""Owner-only player roster and removal API tests."""
import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "layers" / "stand_common" / "python"))
sys.path.insert(0, str(ROOT / "src" / "stand_prod_game_fn_assign"))
from tests.email_fake import FakeDynamoError


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


with patch("boto3.resource"), patch("boto3.client"):
    game = load("game_players_lambda", ROOT / "src" / "stand_prod_game_fn_game" / "lambda_function.py")
    assign = load("game_players_assign_lambda", ROOT / "src" / "stand_prod_game_fn_assign" / "lambda_function.py")
game.ClientError = FakeDynamoError


def event(method, path, body=None, query=None, owner="owner"):
    return {
        "rawPath": path,
        "requestContext": {
            "http": {"method": method},
            "authorizer": {"jwt": {"claims": {"sub": owner}}},
        },
        "queryStringParameters": query or {},
        "body": json.dumps(body) if body is not None else None,
    }


def response_body(response):
    return json.loads(response["body"])


def test_get_players_returns_minimal_identity_fields_and_all_pages():
    game_table = MagicMock()
    game_table.get_item.return_value = {"Item": {"gameId": "ABC123", "ownerUserId": "owner"}}
    player_table = MagicMock()
    player_table.query.side_effect = [
        {
            "Items": [{"playerId": 1, "username": " Ana ", "email": "ana@example.com"}],
            "LastEvaluatedKey": {"gameId": "ABC123", "playerId": 1},
        },
        {"Items": [{"playerId": 2, "instagramUsername": "@luis", "phone": "123"}]},
    ]

    with patch.object(game, "games_table", game_table), patch.object(game, "gp_table", player_table):
        result = game.lambda_handler(
            event("GET", "/v1/game/players", query={"gameId": "ABC123"}), None
        )

    assert result["statusCode"] == 200
    assert response_body(result) == {
        "ok": True,
        "players": [
            {"playerId": 1, "username": "Ana"},
            {"playerId": 2, "username": "@luis"},
        ],
    }
    assert "ProjectionExpression" in player_table.query.call_args_list[0].kwargs


@pytest.mark.parametrize(
    ("owner", "game_item", "expected_status"),
    [
        ("owner", None, 404),
        ("other", {"gameId": "ABC123", "ownerUserId": "owner"}, 403),
    ],
)
def test_get_players_requires_an_existing_owned_game(owner, game_item, expected_status):
    game_table = MagicMock()
    game_table.get_item.return_value = {"Item": game_item}
    player_table = MagicMock()

    with patch.object(game, "games_table", game_table), patch.object(game, "gp_table", player_table):
        result = game.lambda_handler(
            event("GET", "/v1/game/players", query={"gameId": "ABC123"}, owner=owner), None
        )

    assert result["statusCode"] == expected_status
    player_table.query.assert_not_called()


def test_delete_player_transaction_removes_raffle_result_and_updates_counts():
    game_item = {
        "gameId": "ABC123",
        "ownerUserId": "owner",
        "playersCount": 2,
        "validatedCount": 1,
        "raffleWinners": [{"playerId": 7, "username": "Ana"}, {"playerId": 8, "username": "Luis"}],
    }
    game_table = MagicMock()
    game_table.get_item.return_value = {"Item": game_item}
    player_table = MagicMock()
    player_table.get_item.return_value = {
        "Item": {"gameId": "ABC123", "playerId": 7, "validated": True, "type": {"T1MER": {"score": 5}}},
    }
    transaction_client = MagicMock()
    dynamodb = MagicMock()
    dynamodb.meta.client = transaction_client

    with (
        patch.object(game, "games_table", game_table),
        patch.object(game, "gp_table", player_table),
        patch.object(game, "dynamodb", dynamodb),
    ):
        result = game.lambda_handler(
            event("DELETE", "/v1/game/players", {"gameId": "ABC123", "playerId": 7}), None
        )

    assert result["statusCode"] == 200
    transaction_client.transact_write_items.assert_called_once()
    actions = transaction_client.transact_write_items.call_args.kwargs["TransactItems"]
    update = actions[0]["Update"]
    assert update["ExpressionAttributeValues"][":playersCount"] == {"N": "1"}
    assert update["ExpressionAttributeValues"][":validatedCount"] == {"N": "0"}
    assert update["ExpressionAttributeValues"][":lastPlayerId"] == {"N": "7"}
    assert update["ExpressionAttributeValues"][":winners"] == {
        "L": [{"M": {"playerId": {"N": "8"}, "username": {"S": "Luis"}}}],
    }
    assert actions[1]["Delete"]["Key"] == {
        "gameId": {"S": "ABC123"},
        "playerId": {"N": "7"},
    }


def test_delete_player_also_removes_its_verified_email_mapping():
    game_table = MagicMock()
    game_table.get_item.return_value = {
        "Item": {"gameId": "ABC123", "ownerUserId": "owner", "playersCount": 1, "raffleWinners": []}
    }
    player_table = MagicMock()
    player_table.get_item.return_value = {
        "Item": {"gameId": "ABC123", "playerId": 7, "email": "ana@example.com"},
    }
    identity_table = MagicMock()
    identity_table.get_item.return_value = {"Item": {"id": "identity-key", "playerId": 7}}
    dynamodb = MagicMock()
    dynamodb.Table.return_value = identity_table
    dynamodb.meta.client = MagicMock()

    with (
        patch.object(game, "games_table", game_table),
        patch.object(game, "gp_table", player_table),
        patch.object(game, "dynamodb", dynamodb),
        patch.object(game, "EMAIL_VERIFICATION_TABLE", "verification"),
        patch.object(game, "identity_id", return_value="identity-key"),
    ):
        result = game.lambda_handler(
            event("DELETE", "/v1/game/players", {"gameId": "ABC123", "playerId": 7}), None
        )

    assert result["statusCode"] == 200
    actions = dynamodb.meta.client.transact_write_items.call_args.kwargs["TransactItems"]
    assert actions[2]["Delete"]["TableName"] == "verification"
    assert actions[2]["Delete"]["Key"] == {"id": {"S": "identity-key"}}
    assert actions[2]["Delete"]["ConditionExpression"] == "playerId = :removedPlayerId"


@pytest.mark.parametrize("player_id", [None, True, 0, -1, "1.5", "01"])
def test_delete_player_rejects_invalid_ids_without_writes(player_id):
    result = game.lambda_handler(
        event("DELETE", "/v1/game/players", {"gameId": "ABC123", "playerId": player_id}), None
    )
    assert result["statusCode"] == 400


def test_delete_player_returns_not_found_without_transaction():
    game_table = MagicMock()
    game_table.get_item.return_value = {"Item": {"gameId": "ABC123", "ownerUserId": "owner"}}
    player_table = MagicMock()
    player_table.get_item.return_value = {}
    dynamodb = MagicMock()

    with (
        patch.object(game, "games_table", game_table),
        patch.object(game, "gp_table", player_table),
        patch.object(game, "dynamodb", dynamodb),
    ):
        result = game.lambda_handler(
            event("DELETE", "/v1/game/players", {"gameId": "ABC123", "playerId": 7}), None
        )

    assert result["statusCode"] == 404
    dynamodb.meta.client.transact_write_items.assert_not_called()


def test_delete_player_returns_conflict_when_transaction_conditions_change():
    game_table = MagicMock()
    game_table.get_item.return_value = {
        "Item": {"gameId": "ABC123", "ownerUserId": "owner", "playersCount": 1, "raffleWinners": []}
    }
    player_table = MagicMock()
    player_table.get_item.return_value = {"Item": {"gameId": "ABC123", "playerId": 7}}
    dynamodb = MagicMock()
    dynamodb.meta.client.transact_write_items.side_effect = FakeDynamoError(
        "TransactionCanceledException",
        reasons=[{"Code": "ConditionalCheckFailed"}, {"Code": "None"}],
    )

    with (
        patch.object(game, "games_table", game_table),
        patch.object(game, "gp_table", player_table),
        patch.object(game, "dynamodb", dynamodb),
    ):
        result = game.lambda_handler(
            event("DELETE", "/v1/game/players", {"gameId": "ABC123", "playerId": 7}), None
        )

    assert result["statusCode"] == 409


def test_last_player_id_does_not_reuse_deleted_high_water_mark():
    game_table = MagicMock()
    game_table.get_item.return_value = {"Item": {"lastPlayerId": 11}}
    player_table = MagicMock()
    player_table.query.return_value = {"Items": [{"playerId": 4}]}
    with patch.object(assign, "games_table", game_table), patch.object(assign, "gp_table", player_table):
        assert assign._get_last_player_id("ABC123") == 11
