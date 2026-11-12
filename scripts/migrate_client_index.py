"""Rebuild one operator's verified-email client index; writes require --apply."""
import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Key

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "layers" / "stand_common" / "python"))

from stand_common import client_index
from stand_common.email_verification import identity_id
from stand_common.utils import normalize_email


def _read_pages(table, **options):
    while True:
        page = table.query(**options)
        yield from page.get("Items") or []
        cursor = page.get("LastEvaluatedKey")
        if not cursor:
            return
        options["ExclusiveStartKey"] = cursor


def _games(table, owner_id):
    return sorted(
        (row for row in _read_pages(
            table, IndexName="gsi-ownerUserId",
            KeyConditionExpression=Key("ownerUserId").eq(owner_id),
        ) if row.get("ownerUserId") == owner_id and not row.get("deletedAt")),
        key=lambda row: row["gameId"],
    )


def _players(table, game_id):
    return sorted(
        _read_pages(table, KeyConditionExpression=Key("gameId").eq(game_id), ConsistentRead=True),
        key=lambda row: int(row["playerId"]),
    )


def _verified_ids(dynamo, game_id, players, secret):
    grouped = {}
    for player in players:
        email = player.get("email")
        normalized = normalize_email(email) if isinstance(email, str) else ""
        if normalized:
            grouped.setdefault(normalized, []).append(player)
    verified, ambiguous, unverified = set(), set(), set()
    table = dynamo.Table(os.environ["EMAIL_VERIFICATION_TABLE"])
    for email, records in grouped.items():
        mapping = table.get_item(
            Key={"id": identity_id(secret, game_id, email)}, ConsistentRead=True,
        ).get("Item")
        try:
            mapped_player = int(mapping["playerId"]) if mapping else None
        except (KeyError, TypeError, ValueError, OverflowError):
            mapped_player = None
        if len(records) > 1:
            candidates = [row for row in records if int(row["playerId"]) == mapped_player]
            if len(candidates) == 1:
                chosen = int(candidates[0]["playerId"])
                verified.add(chosen)
                ambiguous.update(int(row["playerId"]) for row in records if int(row["playerId"]) != chosen)
            else:
                ambiguous.update(int(row["playerId"]) for row in records)
            continue
        player = records[0]
        player_id = int(player["playerId"])
        if (client_index.has_verified_email(player)
                or mapped_player == player_id):
            verified.add(player_id)
        else:
            unverified.add(player_id)
    return verified, ambiguous, unverified


def _save_checkpoint(path, state):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False,
    ) as temporary:
        json.dump(state, temporary, separators=(",", ":"), sort_keys=True)
        temporary.write("\n")
        temp_path = Path(temporary.name)
    os.replace(temp_path, path)


def migrate(dynamo, owner_id, secret, checkpoint_path, apply=False):
    mode = "apply" if apply else "dry-run"
    state = {}
    if checkpoint_path.exists():
        state = json.loads(checkpoint_path.read_text(encoding="utf-8"))
        if state.get("ownerUserId") != owner_id:
            raise ValueError("Checkpoint belongs to another owner.")
        if state.get("mode") != mode:
            state = {}
    games_table = dynamo.Table(os.environ.get("GAMES_TABLE", "stand-prod-game-table"))
    players_table = dynamo.Table(os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table"))
    games = _games(games_table, owner_id)
    counts = {"games": len(games), "indexed": 0, "alreadyIndexed": 0, "wouldIndex": 0,
              "ambiguous": 0, "unverified": 0}
    resume_game = state.get("gameId")
    resume_player = state.get("lastPlayerId")
    for game in games:
        game_id = game["gameId"]
        if resume_game and (game_id < resume_game or (game_id == resume_game and resume_player is None)):
            continue
        players = _players(players_table, game_id)
        verified, ambiguous, unverified = _verified_ids(dynamo, game_id, players, secret)
        for player in players:
            player_id = int(player["playerId"])
            if (resume_game == game_id and resume_player is not None
                    and player_id <= int(resume_player)):
                continue
            if player_id in verified:
                if apply:
                    created = client_index.index_player(
                        dynamo, owner_id, game_id, player, secret, allow_legacy_verified=True,
                    )
                    counts["indexed" if created else "alreadyIndexed"] += 1
                else:
                    counts["wouldIndex"] += 1
            elif player_id in ambiguous:
                counts["ambiguous"] += 1
            elif player_id in unverified:
                counts["unverified"] += 1
            _save_checkpoint(checkpoint_path, {
                "ownerUserId": owner_id, "mode": mode,
                "gameId": game_id, "lastPlayerId": player_id,
            })
        _save_checkpoint(checkpoint_path, {
            "ownerUserId": owner_id, "mode": mode,
            "gameId": game_id, "lastPlayerId": None,
        })
        resume_player = None
    _save_checkpoint(checkpoint_path, {
        "ownerUserId": owner_id, "mode": mode, "completed": True,
    })
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner-id", required=True)
    parser.add_argument("--checkpoint", required=True, type=Path)
    parser.add_argument("--apply", action="store_true", help="Write index rows (default is a read-only dry run).")
    args = parser.parse_args()
    secret = os.environ.get("PLAYER_TOKEN_SECRET", "")
    if len(secret) < 32:
        parser.error("PLAYER_TOKEN_SECRET must be configured with the deployed secret.")
    if not os.environ.get("CLIENT_INDEX_TABLE"):
        parser.error("CLIENT_INDEX_TABLE must name the deployed client index table.")
    dynamo = boto3.resource("dynamodb")
    result = migrate(dynamo, args.owner_id, secret, args.checkpoint, args.apply)
    print(json.dumps({"mode": "apply" if args.apply else "dry-run", **result}, sort_keys=True))


if __name__ == "__main__":
    main()
