import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from stand_common import client_index, clients_api
from stand_common.email_verification import identity_id
from tests.email_fake import FakeDynamo, FakeDynamoError


ROOT = Path(__file__).parent.parent
SECRET = "client-index-test-secret-long-enough"


@pytest.fixture(autouse=True)
def config(monkeypatch):
    monkeypatch.setenv("CLIENT_INDEX_TABLE", "clients")
    monkeypatch.setenv("EMAIL_VERIFICATION_TABLE", "verification")
    monkeypatch.setenv("STACK_PRIZES_TABLE", "prizes")
    monkeypatch.setenv("PLAYER_TOKEN_SECRET", SECRET)
    monkeypatch.setattr(client_index, "ClientError", FakeDynamoError)
    monkeypatch.setattr(clients_api, "ClientError", FakeDynamoError)


def participant(game_id="ABC123", player_id=1, **fields):
    return {
        "gameId": game_id, "playerId": player_id, "email": " Ana@Example.com ",
        "verifiedAt": "2026-01-01T00:00:00Z",
        "verificationMethod": "email-code", "username": "Ana",
        "phone": "+34600000000", **fields,
    }


def response_body(response):
    return json.loads(response["body"])


def test_index_uses_owner_scoped_verified_email_and_is_idempotent(monkeypatch):
    dynamo = FakeDynamo()
    monkeypatch.setenv("CLIENT_INDEX_TABLE", "clients")
    row = participant()

    assert client_index.client_id(SECRET, "owner-a", row["email"]) == client_index.client_id(
        SECRET, "owner-a", "ana@example.com",
    )
    assert client_index.client_id(SECRET, "owner-a", row["email"]) != client_index.client_id(
        SECRET, "owner-b", row["email"],
    )
    assert client_index.index_player(dynamo, "owner-a", "ABC123", row, SECRET)
    assert not client_index.index_player(dynamo, "owner-a", "ABC123", row, SECRET)
    items = dynamo.rows("clients")
    profile = next(item for item in items if item["recordKey"] == "PROFILE")
    reference = next(item for item in items if item["recordKey"] != "PROFILE")
    assert profile == {
        "clientId": profile["clientId"], "recordKey": "PROFILE",
        "ownerUserId": "owner-a", "email": "ana@example.com", "name": "Ana",
        "phone": "+34600000000", "participationCount": 1,
    }
    assert "email" not in reference and reference["gameId"] == "ABC123"


def test_unverified_participant_is_not_indexed(monkeypatch):
    dynamo = FakeDynamo()
    row = participant(verifiedAt=None)
    assert not client_index.index_player(dynamo, "owner-a", "ABC123", row, SECRET)
    assert dynamo.rows("clients") == []


def test_delete_reference_keeps_profile_until_last_participation(monkeypatch):
    dynamo = FakeDynamo()
    first = participant(game_id="ABC123", player_id=1)
    second = participant(game_id="DEF456", player_id=4)
    client_index.index_player(dynamo, "owner-a", first["gameId"], first, SECRET)
    client_index.index_player(dynamo, "owner-a", second["gameId"], second, SECRET)

    assert client_index.remove_participation(dynamo, "owner-a", "ABC123", first, SECRET)
    remaining = dynamo.rows("clients")
    profile = next(item for item in remaining if item["recordKey"] == "PROFILE")
    assert profile["participationCount"] == 1
    assert len(remaining) == 2

    assert client_index.remove_participation(dynamo, "owner-a", "DEF456", second, SECRET)
    assert dynamo.rows("clients") == []


def test_migration_accepts_markers_and_exact_mapping_but_skips_ambiguous_duplicates(monkeypatch):
    script_path = ROOT / "scripts" / "migrate_client_index.py"
    spec = importlib.util.spec_from_file_location("migrate_client_index", script_path)
    migration = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(migration)
    dynamo = FakeDynamo()
    rows = [
        participant(player_id=1, email="verified@example.com"),
        participant(player_id=2, email="old@example.com", verifiedAt=None, verificationMethod=None),
        participant(player_id=3, email="duplicate@example.com"),
        participant(player_id=4, email="DUPLICATE@example.com"),
    ]
    mapping_email = "duplicate@example.com"
    dynamo.seed("verification", {
        "id": identity_id(SECRET, "ABC123", mapping_email), "playerId": 4,
    })

    verified, ambiguous, unverified = migration._verified_ids(dynamo, "ABC123", rows, SECRET)
    assert verified == {1, 4}
    assert ambiguous == {3}
    assert unverified == {2}


def test_historical_rebuild_is_repeatable_and_groups_participations(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location(
        "migrate_client_index_rebuild", ROOT / "scripts" / "migrate_client_index.py",
    )
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    monkeypatch.setenv("GAMES_TABLE", "games")
    monkeypatch.setenv("GAMEPLAYER_TABLE", "players")
    games = [{"gameId": gid, "ownerUserId": "owner"} for gid in ("ABC123", "DEF456")]
    monkeypatch.setattr(migration, "_games", lambda _table, owner: games if owner == "owner" else [])
    monkeypatch.setattr(migration, "_players", lambda _table, gid: [participant(game_id=gid)])
    dynamo = FakeDynamo()
    checkpoint = tmp_path / "checkpoint.json"
    dry_run = migration.migrate(dynamo, "owner", SECRET, checkpoint)
    assert dry_run["wouldIndex"] == 2
    assert dynamo.rows("clients") == []
    applied = migration.migrate(dynamo, "owner", SECRET, checkpoint, apply=True)
    assert applied["indexed"] == 2
    repeated = migration.migrate(dynamo, "owner", SECRET, checkpoint, apply=True)
    assert repeated["indexed"] == 0
    assert repeated["alreadyIndexed"] == 2
    profiles = [row for row in dynamo.rows("clients") if row["recordKey"] == "PROFILE"]
    assert len(profiles) == 1
    assert profiles[0]["participationCount"] == 2


def test_owner_api_lists_only_profiles_for_the_authenticated_operator():
    dynamo = MagicMock()
    table = dynamo.Table.return_value
    table.query.return_value = {"Items": [
        {"recordKey": "PROFILE", "clientId": "a" * 64, "ownerUserId": "owner",
         "email": "ana@example.com", "participationCount": 2},
        {"recordKey": "PROFILE", "clientId": "b" * 64, "ownerUserId": "other",
         "email": "leak@example.com", "participationCount": 1},
        {"recordKey": "GAME#ABC123#PLAYER#0001", "clientId": "a" * 64,
         "gameId": "ABC123", "playerId": 1},
    ]}
    event = {
        "requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
        "queryStringParameters": {},
    }

    response = clients_api.handle(event, "GET", dynamo, MagicMock(), MagicMock())

    assert response["statusCode"] == 200
    payload = response_body(response)
    assert [row["email"] for row in payload["clients"]] == ["ana@example.com"]
    assert payload["nextCursor"] is None
    assert table.query.call_args.kwargs["IndexName"] == "owner-clients"
    assert table.query.call_args.kwargs["Limit"] == 25


@pytest.mark.parametrize("requested,expected", [(None, 25), ("25", 25), ("50", 25), ("100", 25), ("1", 1)])
def test_client_pages_are_capped_at_25_including_older_frontends(requested, expected):
    params = {} if requested is None else {"limit": requested}
    assert clients_api._page_size(params) == expected


@pytest.mark.parametrize("requested", ["0", "101", "-1", "abc", 25])
def test_invalid_client_page_sizes_are_rejected(requested):
    with pytest.raises(ValueError, match="Invalid page size"):
        clients_api._page_size({"limit": requested})


def test_client_list_passes_continuation_cursor_without_loading_all_clients():
    dynamo = MagicMock()
    cursor = {"clientId": "a" * 64, "recordKey": "PROFILE", "ownerUserId": "owner"}
    dynamo.Table.return_value.query.return_value = {"Items": [], "LastEvaluatedKey": cursor}
    event = {
        "requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
        "queryStringParameters": {"cursor": clients_api._cursor_encode(cursor), "limit": "50"},
    }
    response = clients_api.handle(event, "GET", dynamo, MagicMock(), MagicMock())
    assert response["statusCode"] == 200
    assert clients_api._cursor_decode(response_body(response)["nextCursor"]) == cursor
    options = dynamo.Table.return_value.query.call_args.kwargs
    assert options["Limit"] == 25
    assert options["ExclusiveStartKey"] == cursor
    dynamo.Table.return_value.query.assert_called_once()


def test_same_email_across_games_has_one_profile_and_two_participations():
    dynamo = FakeDynamo()
    first = participant()
    second = participant(game_id="DEF456", player_id=3, email="ana@example.com")
    for row in (first, second, first, second):
        client_index.index_player(dynamo, "owner", row["gameId"], row, SECRET)
    profiles = [row for row in dynamo.rows("clients") if row["recordKey"] == "PROFILE"]
    references = [row for row in dynamo.rows("clients") if row["recordKey"] != "PROFILE"]
    assert len(profiles) == 1
    assert profiles[0]["participationCount"] == 2
    assert len(references) == 2


@pytest.mark.parametrize("search", ["lorena", "VILLENA", "lorena@example", "622810314", "+34 622 810 314"])
def test_directory_searches_all_index_pages_and_only_owners_profiles(search):
    dynamo = MagicMock()
    cursor = {"clientId": "a" * 64, "recordKey": "PROFILE", "ownerUserId": "owner"}
    dynamo.Table.return_value.query.side_effect = [
        {"Items": [], "LastEvaluatedKey": cursor},
        {"Items": [
            {"clientId": "b" * 64, "recordKey": "PROFILE", "ownerUserId": "owner",
             "name": "Loréna Villena", "email": "lorena@example.com", "phone": "+34 622810314",
             "participationCount": 2},
            {"clientId": "c" * 64, "recordKey": "PROFILE", "ownerUserId": "other",
             "name": "Lorena", "email": "other@example.com"},
        ]},
    ]
    response = clients_api._directory({"page": "1", "search": search}, dynamo, "owner")
    body = response_body(response)
    assert response["statusCode"] == 200
    assert body["total"] == 1
    assert body["clients"][0]["id"] == "b" * 64
    assert dynamo.Table.return_value.query.call_args.kwargs["ExclusiveStartKey"] == cursor


def test_directory_counts_and_slices_unique_clients_in_stable_order():
    dynamo = MagicMock()
    rows = [{"clientId": f"{number:064x}", "recordKey": "PROFILE", "ownerUserId": "owner",
             "name": f"Client {number:03d}", "email": f"{number}@example.com",
             "participationCount": 1} for number in range(52)]
    dynamo.Table.return_value.query.return_value = {"Items": list(reversed(rows))}
    for number, length in [(1, 25), (2, 25), (3, 2), (99, 2)]:
        body = response_body(clients_api._directory({"page": str(number)}, dynamo, "owner"))
        assert body["total"] == 52 and len(body["clients"]) == length
        assert body["page"] == min(number, 3)
        assert body["clients"][0]["id"] == rows[(min(number, 3) - 1) * 25]["clientId"]
    empty = response_body(clients_api._directory({"page": "2", "search": "missing"}, dynamo, "owner"))
    assert empty["total"] == 0 and empty["page"] == 1 and empty["clients"] == []


@pytest.mark.parametrize("params", [{"page": "0"}, {"page": "-1"}, {"page": "abc"},
                                    {"page": "9999999999"}, {"page": "1", "search": "x" * 201}])
def test_directory_rejects_invalid_input_without_reading(params):
    dynamo = MagicMock()
    assert clients_api._directory(params, dynamo, "owner")["statusCode"] == 400
    dynamo.Table.assert_not_called()


def test_client_history_separates_kivly_assignment_from_stand_collection(monkeypatch):
    monkeypatch.setenv("CLIENT_INDEX_TABLE", "clients")
    monkeypatch.setenv("STACK_PRIZES_TABLE", "prizes")
    participant_row = participant()
    customer_id = client_index.client_id(SECRET, "owner", participant_row["email"])
    dynamo = MagicMock()
    dynamo.Table.return_value.query.return_value = {"Items": [
        {"clientId": customer_id, "recordKey": "PROFILE", "ownerUserId": "owner",
         "email": "ana@example.com", "name": "Ana", "phone": "+34600000000",
         "participationCount": 1},
        {"clientId": customer_id, "recordKey": "GAME#ABC123#PLAYER#00000000000000000001",
         "gameId": "ABC123", "playerId": 1},
    ]}
    games = MagicMock()
    games.get_item.return_value = {"Item": {
        "gameId": "ABC123", "ownerUserId": "owner", "gameName": "Feria", "gameType": "STACK",
    }}
    players = MagicMock()
    players.get_item.return_value = {"Item": participant_row}
    awards = [
        {"id": "award_kivly", "playerId": 1, "prize": {"source": "kivly", "name": "Club"},
         "state": "confirmed", "emailState": "accepted"},
        {"id": "award_stand", "playerId": 1, "prize": {"source": "stand", "name": "Entrada"},
         "state": "delivered", "deliveredAt": "2026-01-02T00:00:00Z", "token": "must-not-leak"},
    ]
    with patch.object(clients_api.stack_prizes, "records", return_value=awards), \
         patch.object(clients_api.stack_prizes, "image_url", return_value="https://example.com/prize.png") as image_url:
        response = clients_api.handle({
            "requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
            "pathParameters": {"clientId": customer_id},
        }, "GET", dynamo, games, players)

    assert response["statusCode"] == 200
    payload = response_body(response)
    indexed = {award["id"]: award for award in payload["participations"][0]["awards"]}
    assert indexed["award_kivly"]["kivlyAssignmentState"] == "confirmed"
    assert indexed["award_kivly"]["collected"] is False
    assert indexed["award_stand"]["collected"] is True
    assert indexed["award_stand"]["imageUrl"] == "https://example.com/prize.png"
    assert image_url.call_args.args[1]["ownerUserId"] == "owner"
    assert "token" not in json.dumps(payload)
    assert "customerId" not in json.dumps(payload)


def test_client_history_does_not_read_another_operators_game():
    dynamo = MagicMock()
    cid = "c" * 64
    dynamo.Table.return_value.query.return_value = {"Items": [
        {"clientId": cid, "recordKey": "PROFILE", "ownerUserId": "owner",
         "email": "ana@example.com", "participationCount": 1},
        {"clientId": cid, "recordKey": "GAME#ABC123#PLAYER#0001",
         "gameId": "ABC123", "playerId": 1},
    ]}
    games = MagicMock()
    games.get_item.return_value = {"Item": {
        "gameId": "ABC123", "ownerUserId": "another-owner",
    }}
    response = clients_api.handle({
        "requestContext": {"authorizer": {"jwt": {"claims": {"sub": "owner"}}}},
        "pathParameters": {"clientId": cid},
    }, "GET", dynamo, games, MagicMock())
    assert response["statusCode"] == 409
    assert response_body(response)["error"] == "ClientHistoryChanged"
