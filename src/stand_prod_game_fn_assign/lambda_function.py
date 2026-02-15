import os
import json
import random
import time
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# ========= ENV VARS =========
GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table")
IG_SENDER_LAMBDA = os.environ.get("IG_SENDER_LAMBDA", "instagram-sender")
QUIZ_LAMBDA = os.environ.get("QUIZ_LAMBDA", "stand-prod-game-fn-quiz")


# Optional GSI
GSI_INSTAGRAM_PSID = os.environ.get("GSI_INSTAGRAM_PSID", "gsi-instagramPSID")

# ========= AWS =========
dynamo_r = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")

games_table = dynamo_r.Table(GAMES_TABLE)
gp_table = dynamo_r.Table(GAMEPLAYER_TABLE)

# ========= Helpers =========

def log(msg, obj=None):
    if obj is not None:
        print(json.dumps({"msg": msg, "data": obj}, ensure_ascii=False))
    else:
        print(json.dumps({"msg": msg}, ensure_ascii=False))

def _json_sanitize(obj):
    """Convert Dynamo Decimals to int/float recursively (safe for logging/returns)."""
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, dict):
        return {k: _json_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_sanitize(v) for v in obj]
    return obj

def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def _code4() -> int:
    return random.randint(1000, 9999)

def _send_bulk_messages(messages):
    """
    messages: [{ "psid": "...", "text": "..." }, { "psid": "...", "image_url": "..." }, ...]
    """
    if not messages:
        return

    payload = {"messages": messages}
    try:
        lambda_client.invoke(
            FunctionName=IG_SENDER_LAMBDA,
            InvocationType="Event",
            Payload=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        )
    except Exception as e:
        log("assign_send_bulk_error", {"error": repr(e), "count": len(messages)})

def _send_single_dm(psid: str, text: str):
    if not psid:
        return
    _send_bulk_messages([{"psid": psid, "text": text}])

def _get_game_meta(game_id: str) -> dict | None:
    resp = games_table.get_item(Key={"gameId": game_id})
    return resp.get("Item")

def _ensure_game_counters(game_id: str):
    """
    Inicializa counters si no existen. Idempotente (compat con games antiguos).
    """
    games_table.update_item(
        Key={"gameId": game_id},
        UpdateExpression=(
            "SET playersCount = if_not_exists(playersCount, :z), "
            "validatedCount = if_not_exists(validatedCount, :z)"
        ),
        ExpressionAttributeValues={":z": 0},
    )

def _reserve_player_slot(game_id: str, max_players: int, now_iso: str) -> bool:
    """
    Incrementa playersCount de forma atómica SOLO si no excede maxPlayers.
    Devuelve True si reserva OK, False si límite alcanzado.
    """
    try:
        games_table.update_item(
            Key={"gameId": game_id},
            UpdateExpression="ADD playersCount :one SET lastJoinAt = :now, updatedAt = :now",
            ConditionExpression="attribute_not_exists(playersCount) OR playersCount < :max",
            ExpressionAttributeValues={":one": 1, ":now": now_iso, ":max": int(max_players)},
        )
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise

def _rollback_player_slot(game_id: str, now_iso: str):
    """
    Rollback best-effort del contador si algo falla tras reservar plaza.
    """
    games_table.update_item(
        Key={"gameId": game_id},
        UpdateExpression="ADD playersCount :neg SET updatedAt = :now",
        ExpressionAttributeValues={":neg": -1, ":now": now_iso},
    )


def _get_last_player_id(game_id: str) -> int:
    """
    Query descending by SK to get the last assigned playerId.
    Returns 0 if none.
    """
    resp = gp_table.query(
        KeyConditionExpression=Key("gameId").eq(game_id),
        ProjectionExpression="playerId",
        ScanIndexForward=False,
        Limit=1,
    )
    items = resp.get("Items") or []
    if not items:
        return 0
    pid = items[0].get("playerId")
    return int(pid) if not isinstance(pid, Decimal) else int(pid)

def _find_existing_player_by_psid(game_id: str, psid: str) -> dict | None:
    """
    Preferred: query GSI instagramPSID (PK) + gameId (SK).
    Fallback: query the partition and search (OK for MVP; partitions are small).
    """
    # Try GSI first
    try:
        resp = gp_table.query(
            IndexName=GSI_INSTAGRAM_PSID,
            KeyConditionExpression=Key("instagramPSID").eq(psid) & Key("gameId").eq(game_id),
            Limit=1,
        )
        items = resp.get("Items") or []
        return items[0] if items else None
    except ClientError as e:
        # Index might not exist yet -> fallback
        log("assign_psid_gsi_unavailable_fallback", {"error": str(e), "index": GSI_INSTAGRAM_PSID})

    # Fallback: query partition and scan in-memory (no Dynamo Scan)
    try:
        kwargs = {
            "KeyConditionExpression": Key("gameId").eq(game_id),
            "ProjectionExpression": "gameId, playerId, instagramPSID, instagramUsername, joinedAt, validated, validationCode, type",
        }
        while True:
            page = gp_table.query(**kwargs)
            for it in page.get("Items") or []:
                if (it.get("instagramPSID") or "") == psid:
                    return it
            lek = page.get("LastEvaluatedKey")
            if not lek:
                break
            kwargs["ExclusiveStartKey"] = lek
    except ClientError as e:
        log("assign_psid_fallback_query_failed", {"error": str(e)})

    return None

def _put_player(item: dict) -> None:
    # Conditional protects races on playerId within same partition
    gp_table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(playerId)",
    )


def _invoke_quiz_start(game_id: str, psid: str):
    if not QUIZ_LAMBDA:
        log("assign_quiz_lambda_missing", {"gameId": game_id})
        return
    try:
        lambda_client.invoke(
            FunctionName=QUIZ_LAMBDA,
            InvocationType="Event",
            Payload=json.dumps({
                "kind": "quiz_start",
                "gameId": game_id,
                "psid": [psid],
            }, ensure_ascii=False).encode("utf-8"),
        )
    except Exception as e:
        log("assign_quiz_invoke_error", {"error": repr(e), "quizLambda": QUIZ_LAMBDA})


def _invoke_quiz_start_sync(game_id: str, psid: str) -> dict:
    if not QUIZ_LAMBDA:
        return {"ok": False, "error": "no_quiz_config"}
    try:
        resp = lambda_client.invoke(
            FunctionName=QUIZ_LAMBDA,
            InvocationType="RequestResponse",
            Payload=json.dumps({
                "kind": "quiz_start",
                "gameId": game_id,
                "psid": [psid],
            }, ensure_ascii=False).encode("utf-8"),
        )
        payload_bytes = resp.get("Payload").read() if resp.get("Payload") else b""
        payload_str = payload_bytes.decode("utf-8", errors="replace").strip()
        out = json.loads(payload_str) if payload_str else {}
        print(out)
        if resp.get("FunctionError"):
            return {"ok": False, "error": "quiz_function_error", "detail": out}
        return out if isinstance(out, dict) else {"ok": False, "error": "quiz_bad_payload"}
    except Exception as e:
        log("assign_quiz_invoke_error", {"error": repr(e), "quizLambda": QUIZ_LAMBDA})
        return {"ok": False, "error": "quiz_invoke_error"}


QUIZ_TAIL = (
    "Antes de jugar, tienes que responder un mini quiz.\n"
    "Cuando lo termines, te enviaré tu código para validar en la pantalla. ✅"
)

def _code_tail(code) -> str:
    return f"🎟️ Tu código para jugar es: {code}\n\nVe a la pantalla, introdúcelo y ¡a jugar! 🚀"



# ========= Assigners registry =========
# Import here (not top) to avoid circular imports
from assigners.empareja2 import assign_empareja2
from assigners.t1mer import assign_t1mer
from assigners.rulet4 import assign_rulet4
from assigners.semaforo import assign_semaforo
from assigners.generic import assign_generic

ASSIGNERS = {
    "EMPAREJA2": assign_empareja2,
    "T1MER": assign_t1mer,
    "RULET4": assign_rulet4,
    "SEMAFORO": assign_semaforo,
}

def lambda_handler(event, context):
    """
    Expects dispatcher event:
    {
      "psid": "USER_PSID",
      "username_at": "@usuario" | None,
      "game_id": "K9R4QM",
      "raw_event": {...}
    }
    """
    try:
        psid = (event or {}).get("psid")
        username_at = (event or {}).get("username_at")
        game_id = (event or {}).get("game_id")

        # 1) sanity
        if not psid or not game_id:
            log("assign_bad_event", {"reason": "missing psid or game_id", "event": event})
            return {"ok": False, "reason": "missing psid or game_id"}

        if not username_at:
            _send_single_dm(
                psid,
                "Necesito tu usuario de Instagram para continuar 😅\n"
                "Responde con tu @usuario exactamente como aparece en Instagram."
            )
            log("assign_missing_username", {"psid": psid, "gameId": game_id})
            return {"ok": False, "reason": "missing username_at"}

        # 2) load game meta
        meta = _get_game_meta(game_id)
        if not meta:
            _send_single_dm(psid, "Ese juego no existe o ya no está disponible.")
            return {"ok": False, "reason": "game_not_found"}

        if not meta.get("isActive", True):
            _send_single_dm(psid, "Este juego ya no está activo.")
            return {"ok": False, "reason": "game_inactive"}

        game_type = (meta.get("gameType") or "UNKNOWN").upper()
        max_players_raw = meta.get("maxPlayers") or 9999
        max_players = int(max_players_raw) if not isinstance(max_players_raw, Decimal) else int(max_players_raw)

        # 3) idempotency: if already joined, do not create again
        existing = _find_existing_player_by_psid(game_id, psid)
        if existing:
            pid = existing.get("playerId")
            _send_single_dm(psid, "Ya estabas dentro del juego ✅\nSi no has completado el quiz, te lo vuelvo a enviar ahora.")
            qr = _invoke_quiz_start_sync(game_id, psid)
            # If quiz is not configured, reveal the code immediately
            if (qr or {}).get("ok") is False and (qr or {}).get("error") == "no_quiz_config":
                code = existing.get("validationCode")
                if code is not None:
                    _send_code_now(psid, code)
            log("assign_already_joined", {"gameId": game_id, "psid": psid, "playerId": pid})
            return _json_sanitize({
                "ok": True,
                "created_new": False,
                "already_joined": True,
                "gameId": game_id,
                "gameType": game_type,
                "player": existing,
            })

        # 4) counters + enforce maxPlayers (atómico, sin contar en gp_table)
        attempts = 6
        now = _iso_now()

        # compat: games antiguos pueden no tener counters
        try:
            _ensure_game_counters(game_id)
        except Exception as e:
            log("assign_ensure_counters_failed", {"error": repr(e), "gameId": game_id})

        reserved = _reserve_player_slot(game_id, max_players, now)
        if not reserved:
            _send_single_dm(
                psid,
                "Este juego ha alcanzado el límite de jugadores 😅\n"
                "Pide al organizador que amplíe el límite (plan 24h) o cree otra partida."
            )
            log("assign_limit_reached_atomic", {"gameId": game_id, "max": max_players})
            return {"ok": False, "reason": "player_limit_reached", "maxPlayers": max_players}

        # 5) allocate playerId + write (retry on race)
        for _ in range(attempts):
            last_pid = _get_last_player_id(game_id)
            new_pid = int(last_pid) + 1

            base_item = {
                "gameId": game_id,
                "playerId": new_pid,
                "instagramPSID": psid,
                "instagramUsername": username_at,
                "joinedAt": now,
                "validated": False,
                "validationCode": _code4(),
            }

            assigner = ASSIGNERS.get(game_type, assign_generic)
            ctx = {
                "gameId": game_id,
                "playerId": new_pid,
                "psid": psid,
                "username_at": username_at,
                "gameType": game_type,
                "gameMeta": meta,
                "now": now,
                "dynamo_r": dynamo_r,
                "lambda_client": lambda_client,
                "log_fn": log,
                "validationCode": base_item["validationCode"],
            }

            # assigner contract: (patch, welcome_header, extra_messages)
            patch, welcome_header, extra_messages = assigner(ctx)

            # merge patch into base_item BEFORE writing
            if patch:
                for k, v in patch.items():
                    base_item[k] = v

            try:
                _put_player(base_item)

                # 1) send extra messages first (images, etc.)
                if extra_messages:
                    _send_bulk_messages(extra_messages)

                # 2) send welcome header (NO quiz mention here)
                if welcome_header:
                    _send_single_dm(psid, welcome_header)

                time.sleep(1)
                # 3) attempt to start quiz (sync)
                qr = _invoke_quiz_start_sync(game_id, psid)
                no_quiz = (qr or {}).get("ok") is False and (qr or {}).get("error") == "no_quiz_config"

                # 4) if no quiz, send code now
                if no_quiz:
                    _send_single_dm(psid, _code_tail(base_item["validationCode"]))

                log("assign_ok", {
                    "gameId": game_id,
                    "psid": psid,
                    "username_at": username_at,
                    "gameType": game_type,
                    "playerId": new_pid,
                    "created_new": True,
                    "no_quiz": no_quiz,
                })

                safe = _json_sanitize(base_item)
                return {
                    "ok": True,
                    "created_new": True,
                    "gameId": game_id,
                    "gameType": game_type,
                    "player": safe,
                    "quizStart": qr,
                }

            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    # race on playerId -> retry (NO rollback, already reserved slot)
                    continue

                # rollback reserva (best-effort)
                try:
                    _rollback_player_slot(game_id, now)
                except Exception as re:
                    log("assign_playersCount_rollback_failed", {"error": repr(re), "gameId": game_id})

                log("assign_put_failed", {"error": str(e), "gameId": game_id})
                return {"ok": False, "reason": "put_failed", "detail": str(e)}
        # si agotamos retries por race: rollback reserva
        try:
            _rollback_player_slot(game_id, now)
        except Exception as re:
            log("assign_playersCount_rollback_failed", {"error": repr(re), "gameId": game_id})

        log("assign_race_exhausted", {"gameId": game_id, "psid": psid})
        return {"ok": False, "reason": "race_condition_retry_exhausted"}


    except Exception as e:
        log("assign_internal_error", {"error": repr(e)})
        return {"ok": False, "reason": "internal_error", "detail": repr(e)}
