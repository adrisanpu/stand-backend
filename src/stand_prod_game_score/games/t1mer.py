# games/t1mer.py
from datetime import datetime, timezone
from decimal import Decimal
from boto3.dynamodb.conditions import Key


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _as_int(x):
    try:
        if isinstance(x, Decimal):
            return int(x)
        return int(x)
    except Exception:
        return None


def _as_float(x):
    try:
        if isinstance(x, Decimal):
            return float(x)
        return float(x)
    except Exception:
        return None


def _get_type_blob(item: dict, game_type_upper: str) -> dict:
    t = item.get("type") or {}
    return t.get((game_type_upper or "").upper()) or {}


def _ensure_type_maps(game_id: str, player_id: int, game_type_upper: str, gp_table):
    """
    Evita errores de paths inválidos creando:
      type
      type.<GAME_TYPE_UPPER>
    (2 updates separados para evitar solapes)
    """
    gk = (game_type_upper or "").upper()
    if not gk:
        raise ValueError("Missing gameType")

    key = {"gameId": game_id, "playerId": int(player_id)}

    # 1) ensure root map
    gp_table.update_item(
        Key=key,
        UpdateExpression="SET #type = if_not_exists(#type, :tinit)",
        ExpressionAttributeNames={"#type": "type"},
        ExpressionAttributeValues={":tinit": {}},
    )

    # 2) ensure per-game map
    gp_table.update_item(
        Key=key,
        UpdateExpression="SET #type.#g = if_not_exists(#type.#g, :ginit)",
        ExpressionAttributeNames={"#type": "type", "#g": gk},
        ExpressionAttributeValues={":ginit": {}},
    )


def store_score(game_id: str, game_type_upper: str, gp_table, payload: dict, log_fn):
    """
    Payload esperado:
    {
      "gameId": "...",
      "playerId": 12,
      "timer": "10.123",
      "score": 123
    }

    Guarda en:
      type.T1MER.timer
      type.T1MER.score
      type.T1MER.scoreUpdatedAt
    """

    gk = (game_type_upper or "").upper()
    if not gk:
        return {"ok": False, "error": "missing_gameType"}

    player_id_raw = payload.get("playerId")
    timer_raw = payload.get("timer")
    score_raw = payload.get("score")

    if player_id_raw is None or timer_raw is None or score_raw is None:
        return {"ok": False, "error": "missing_fields", "message": "Faltan playerId, timer o score."}

    try:
        player_id = int(str(player_id_raw).strip())
    except Exception:
        return {"ok": False, "error": "invalid_playerId", "message": "playerId debe ser numérico."}

    try:
        timer_float = float(str(timer_raw).strip())
    except Exception:
        return {"ok": False, "error": "invalid_timer", "message": "timer debe ser un número (segundos)."}

    try:
        score_int = int(score_raw)
    except Exception:
        return {"ok": False, "error": "invalid_score", "message": "score debe ser entero."}

    # 1) comprobar jugador existe
    resp = gp_table.get_item(Key={"gameId": game_id, "playerId": int(player_id)})
    item = resp.get("Item")
    if not item:
        return {"ok": False, "error": "player_not_found", "message": "No existe ningún jugador con ese playerId."}

    # 2) asegurar mapas type.<GAME>
    _ensure_type_maps(game_id, player_id, gk, gp_table)

    # 3) escribir score/timer
    now = _iso_now()

    gp_table.update_item(
        Key={"gameId": game_id, "playerId": int(player_id)},
        UpdateExpression="SET #type.#g.timer = :t, #type.#g.score = :s, #type.#g.scoreUpdatedAt = :ts",
        ExpressionAttributeNames={"#type": "type", "#g": gk},
        ExpressionAttributeValues={
            ":t": Decimal(str(timer_float)),
            ":s": Decimal(int(score_int)),
            ":ts": now,
        },
    )

    return {
        "ok": True,
        "gameId": game_id,
        "playerId": player_id,
        "username": item.get("instagramUsername", ""),
        "timer": timer_float,
        "score": score_int,
        "scoreUpdatedAt": now,
    }


def get_ranking(game_id: str, game_type_upper: str, gp_table, limit: int, log_fn):
    """
    Ranking T1MER: score ASC (menor mejor)
    Lee todos los players del game (partition pequeña), filtra los que tengan score+timer en type.T1MER.*
    """
    if limit <= 0:
        limit = 10
    if limit > 100:
        limit = 100

    gk = (game_type_upper or "").upper()
    if not gk:
        return {"ok": False, "error": "missing_gameType"}

    # Query todos los jugadores del gameId
    items = []
    last = None
    while True:
        kwargs = {"KeyConditionExpression": Key("gameId").eq(game_id)}
        if last:
            kwargs["ExclusiveStartKey"] = last
        resp = gp_table.query(**kwargs)
        items.extend(resp.get("Items") or [])
        last = resp.get("LastEvaluatedKey")
        if not last:
            break

    ranking = []
    for it in items:
        pid = _as_int(it.get("playerId"))
        if pid is None or pid <= 0:
            continue

        tb = (it.get("type") or {}).get(gk) or {}
        score_val = tb.get("score")
        timer_val = tb.get("timer")

        score_float = _as_float(score_val)
        timer_float = _as_float(timer_val)
        if score_float is None or timer_float is None:
            continue

        ranking.append({
            "playerId": pid,
            "username": it.get("instagramUsername", ""),
            "timer": timer_float,
            "score": score_float,
        })

    ranking.sort(key=lambda x: x["score"])  # ASC = mejor
    ranking = ranking[:limit]
    for idx, entry in enumerate(ranking, start=1):
        entry["rank"] = idx

    return {"ok": True, "gameId": game_id, "gameType": gk, "limit": limit, "results": ranking}
