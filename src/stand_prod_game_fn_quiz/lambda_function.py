import os
import json
import base64
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from stand_common.utils import log, _json_sanitize, _resp, _iso_now, _as_int

# ============ ENV VARS ============
GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table")
# ==================================

dynamo_r = boto3.resource("dynamodb")

games_table = dynamo_r.Table(GAMES_TABLE)
gp_table = dynamo_r.Table(GAMEPLAYER_TABLE)


def _parse_http_event(event: dict):
    method = (
        (event or {}).get("httpMethod")
        or (event or {}).get("requestContext", {}).get("http", {}).get("method", "GET")
    )
    method = (method or "GET").upper()

    if method == "OPTIONS":
        return method, {}, {}

    raw = (event or {}).get("body") or ""
    if raw and (event or {}).get("isBase64Encoded"):
        try:
            raw = base64.b64decode(raw).decode("utf-8")
        except Exception as e:
            log("body_b64_decode_error", {"error": repr(e)})
            raw = ""

    body = {}
    if raw:
        try:
            body = json.loads(raw)
        except Exception as e:
            log("body_json_parse_error", {"error": repr(e), "raw_sample": str(raw)[:200]})
            body = {}

    qs = (event or {}).get("queryStringParameters") or {}
    return method, body, qs


# ----------------- Quiz meta (game-table) -----------------

def _save_quiz_meta(game_id: str, questions: list[dict]):
    """
    Guarda en games table:
      quizOrder: [q1, q2...]
      quizQuestions: { q1: {text, options:[{title,payload}]} ... }
    """
    quiz_order = []
    quiz_questions = {}

    for q in questions:
        qid = str(q.get("id") or "").strip()
        qtext = (q.get("text") or "").strip()
        opts = q.get("options") or []
        if not qid or not qtext or not isinstance(opts, list):
            continue

        quiz_order.append(qid)

        norm_opts = []
        for o in opts:
            title = (o.get("title") or "").strip()
            payload = (o.get("payload") or "").strip()
            if not title or not payload:
                continue
            norm_opts.append({"title": title, "payload": payload})

        quiz_questions[qid] = {"text": qtext, "options": norm_opts}

    if not quiz_order:
        raise ValueError("No valid questions provided")

    games_table.update_item(
        Key={"gameId": game_id},
        UpdateExpression="SET quizOrder = :o, quizQuestions = :q",
        ExpressionAttributeValues={":o": quiz_order, ":q": quiz_questions},
    )
    log("quiz_meta_saved", {"gameId": game_id, "quizOrder": quiz_order})


def _get_quiz_meta(game_id: str):
    resp = games_table.get_item(Key={"gameId": game_id})
    item = resp.get("Item")
    if not item:
        return [], {}, None
    return item.get("quizOrder", []) or [], item.get("quizQuestions", {}) or {}, item


def _prepare_quiz_for_existing_players(game_id: str, game_type_upper: str):
    if not game_type_upper:
        raise ValueError("Missing gameType")

    gk = game_type_upper.upper()
    now = _iso_now()

    # Query todos los players del game (PK=gameId)
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

    updated = 0
    for it in items:
        pid = _as_int(it.get("playerId"))
        if pid is None or pid <= 0:
            continue

        # asegura type
        gp_table.update_item(
            Key={"gameId": game_id, "playerId": int(pid)},
            UpdateExpression="SET #type = if_not_exists(#type, :tinit)",
            ExpressionAttributeNames={"#type": "type"},
            ExpressionAttributeValues={":tinit": {}},
        )

        # asegura type.<gk>
        gp_table.update_item(
            Key={"gameId": game_id, "playerId": int(pid)},
            UpdateExpression="SET #type.#g = if_not_exists(#type.#g, :ginit)",
            ExpressionAttributeNames={"#type": "type", "#g": gk},
            ExpressionAttributeValues={":ginit": {}},
        )

        # defaults quiz
        gp_table.update_item(
            Key={"gameId": game_id, "playerId": int(pid)},
            UpdateExpression=(
                "SET #type.#g.quizRequired = :req, "
                "#type.#g.quizCompleted = :done, "
                "#type.#g.quizCurrentQuestion = :cur, "
                "#type.#g.#qa = if_not_exists(#type.#g.#qa, :qainit), "
                "#type.#g.quizUpdatedAt = :t"
            ),
            ExpressionAttributeNames={"#type": "type", "#g": gk, "#qa": "quizAnswers"},
            ExpressionAttributeValues={
                ":req": True,
                ":done": False,
                ":cur": None,
                ":qainit": {},
                ":t": now,
            },
        )

        updated += 1

    return {"playersSeen": len(items), "playersUpdated": updated}


def _scan_players_in_game(game_id: str):
    out = []
    last = None
    while True:
        kwargs = {"KeyConditionExpression": Key("gameId").eq(game_id)}
        if last:
            kwargs["ExclusiveStartKey"] = last
        resp = gp_table.query(**kwargs)
        out.extend(resp.get("Items") or [])
        last = resp.get("LastEvaluatedKey")
        if not last:
            break
    return out


# ----------------- Quiz state en type.<GAME_TYPE_UPPER> -----------------









# ----------------- Payload parsing -----------------

def _parse_quiz_payload(payload: str):
    """
    Esperado: "{gameId}_{questionId}_{answerId}"
    gameId puede tener underscores.
    """
    if not payload:
        return None, None, None
    if payload.startswith("QR_"):
        return None, None, payload

    parts = payload.split("_")
    if len(parts) < 3:
        return None, None, None

    answer_id = parts[-1]
    question_id = parts[-2]
    game_id = "_".join(parts[:-2])

    if not game_id or not question_id or not answer_id:
        return None, None, None
    return game_id, question_id, answer_id


# ----------------- MAIN handler -----------------

def lambda_handler(event, context):
    # ========== HTTP MODE ==========
    if isinstance(event, dict) and ("httpMethod" in event or "requestContext" in event):
        method, body, qs = _parse_http_event(event)

        if method == "OPTIONS":
            return {"statusCode": 204, "body": ""}

        # -------- GET export --------
        if method == "GET":
            game_id = (qs.get("gameId") or body.get("gameId") or "").strip()
            if not game_id:
                return _resp(400, {"ok": False, "error": "MissingGameId"})

            quiz_order, quiz_questions, meta = _get_quiz_meta(game_id)
            if not meta:
                return _resp(404, {"ok": False, "error": "GameNotFound"})

            game_type = (meta.get("gameType") or "").upper() or "UNKNOWN"
            gk = game_type.upper()

            # 👇 CLAVE: si no hay quiz configurado -> 200 OK con listas vacías
            if not quiz_order:
                players_items = _scan_players_in_game(game_id)
                players_out = []
                for it in players_items:
                    pid = _as_int(it.get("playerId"))
                    if pid is None or pid <= 0:
                        continue
                    players_out.append({
                        "playerId": pid,
                        "quizRequired": False,
                        "quizCompleted": False,
                        "quizCurrentQuestion": None,
                        "quizAnswers": {},
                    })

                return _resp(200, {
                    "ok": True,
                    "quizEnabled": False,
                    "gameId": game_id,
                    "gameType": game_type,
                    "questions": [],
                    "players": players_out,

                    # si tu UI también espera esto para raffle polling:
                    "raffleWinners": [],
                })

            # Normaliza preguntas
            questions_out = []
            for qid in quiz_order:
                qcfg = quiz_questions.get(qid) or {}
                opts_cfg = qcfg.get("options") or []
                opts_out = []
                for opt in opts_cfg:
                    payload = opt.get("payload")
                    _, _, answer_id = _parse_quiz_payload(payload or "")
                    opts_out.append({"title": opt.get("title"), "payload": payload, "answerId": answer_id})
                questions_out.append({"id": qid, "text": qcfg.get("text"), "options": opts_out})

            # Players export
            players_items = _scan_players_in_game(game_id)
            players_out = []
            for it in players_items:
                pid = _as_int(it.get("playerId"))
                if pid is None or pid <= 0:
                    continue

                tb = ((it.get("type") or {}).get(gk) or {})
                players_out.append({
                    "playerId": pid,
                    "quizCompleted": bool(tb.get("quizCompleted", False)),
                    "quizCurrentQuestion": tb.get("quizCurrentQuestion"),
                    "quizAnswers": tb.get("quizAnswers") or {},
                    "quizRequired": bool(tb.get("quizRequired", False)),
                })

            return _resp(200, {
                "ok": True,
                "quizEnabled": True,
                "gameId": game_id,
                "gameType": game_type,
                "questions": questions_out,
                "players": players_out,
                "raffleWinners": [],
            })

        # -------- POST configure --------
        if method == "POST":
            game_id = (body.get("gameId") or "").strip()
            questions = body.get("questions") or []

            if not game_id:
                return _resp(400, {"ok": False, "error": "MissingGameId"})
            if not isinstance(questions, list) or not questions:
                return _resp(400, {"ok": False, "error": "MissingOrInvalidQuestions"})

            quiz_order, quiz_questions, meta = _get_quiz_meta(game_id)
            if not meta:
                return _resp(404, {"ok": False, "error": "GameNotFound"})

            try:
                _save_quiz_meta(game_id, questions)
            except Exception as e:
                log("quiz_meta_error", {"error": repr(e)})
                return _resp(500, {"ok": False, "error": "quiz_meta_error", "message": str(e)})

            game_type = (meta.get("gameType") or "").upper()
            prep = _prepare_quiz_for_existing_players(game_id, game_type)

            return _resp(200, {"ok": True, "gameId": game_id, "questions": len(questions), "prepared": prep})

        return _resp(405, {"ok": False, "error": "method_not_allowed", "method": method})

    return {"ok": True, "ignored": True}
