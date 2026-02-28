import os
import json
import base64
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from stand_common.utils import log, _json_sanitize, _resp, _iso_now, _as_int

# ============ ENV VARS ============
GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
GAMEPLAYER_TABLE = os.environ.get("GAMEPLAYER_TABLE", "stand-prod-gameplayer-table")
IG_SENDER_LAMBDA = os.environ.get("IG_SENDER_LAMBDA", "instagram-sender")
# ==================================

dynamo_r = boto3.resource("dynamodb")
lambda_client = boto3.client("lambda")

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


# ----------------- IG sender -----------------

def _invoke_instagram_sender(messages):
    if not messages:
        return
    if not IG_SENDER_LAMBDA:
        log("igsender_missing_env", {"count": len(messages)})
        return
    try:
        lambda_client.invoke(
            FunctionName=IG_SENDER_LAMBDA,
            InvocationType="Event",
            Payload=json.dumps({"messages": messages}, ensure_ascii=False).encode("utf-8"),
        )
    except Exception as e:
        log("igsender_invoke_error", {"error": repr(e), "count": len(messages)})


def _send_dm(psid: str, text: str) -> bool:
    if not psid or psid == "#":
        return False
    _invoke_instagram_sender([{"psid": psid, "text": text}])
    return True


def _send_quiz_question(psid: str, question_id: str, questions_cfg: dict) -> bool:
    msg = _build_quiz_question_message(psid, question_id, questions_cfg)
    if not msg:
        return False
    _invoke_instagram_sender([msg])
    return True


def _build_quiz_question_message(psid: str, question_id: str, questions_cfg: dict) -> dict | None:
    """Build a single message dict for a quiz question (for batching with intro)."""
    q = questions_cfg.get(question_id) or {}
    text = (q.get("text") or "").strip()
    options = q.get("options", []) or []

    quick_replies = []
    for opt in options:
        title = (opt.get("title") or "").strip()
        payload = (opt.get("payload") or "").strip()
        if not title or not payload:
            continue
        quick_replies.append({"content_type": "text", "title": title, "payload": payload})

    if not quick_replies:
        return {"psid": psid, "text": text} if text else None
    return {"psid": psid, "text": text, "quick_replies": quick_replies}


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


# ----------------- Player access (GSI gsi-instagramPSID) -----------------

def _get_player_by_psid_and_game(psid: str, game_id: str):
    """
    Usa GSI gsi-instagramPSID:
      PK=instagramPSID, SK=gameId
    """
    resp = gp_table.query(
        IndexName="gsi-instagramPSID",
        KeyConditionExpression=Key("instagramPSID").eq(psid) & Key("gameId").eq(game_id),
        Limit=1,
    )
    items = resp.get("Items") or []
    return items[0] if items else None


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

def _get_type_blob(player_item: dict, game_type_upper: str) -> dict:
    t = player_item.get("type") or {}
    return t.get((game_type_upper or "").upper()) or {}


def _set_quiz_state(game_id: str, player_id: int, game_type_upper: str, next_question_id, completed: bool):
    """
    SET type.<g>.quizCurrentQuestion, type.<g>.quizCompleted
    """
    gk = (game_type_upper or "").upper()
    if not gk:
        raise ValueError("Missing gameType")

    now = _iso_now()

    gp_table.update_item(
        Key={"gameId": game_id, "playerId": int(player_id)},
        UpdateExpression=(
            "SET #type.#g.quizCurrentQuestion = :next, "
            "#type.#g.quizCompleted = :done, "
            "#type.#g.quizUpdatedAt = :t"
        ),
        ExpressionAttributeNames={"#type": "type", "#g": gk},
        ExpressionAttributeValues={":next": next_question_id, ":done": bool(completed), ":t": now},
    )
    return now


def _save_quiz_answer(game_id: str, player_id: int, game_type_upper: str, question_id: str, answer_id: str):
    gk = (game_type_upper or "").upper()
    if not gk:
        raise ValueError("Missing gameType")

    now = _iso_now()

    gp_table.update_item(
        Key={"gameId": game_id, "playerId": int(player_id)},
        UpdateExpression="SET #type.#g.#qa.#qid = :ans, #type.#g.quizUpdatedAt = :t",
        ExpressionAttributeNames={
            "#type": "type",
            "#g": gk,
            "#qa": "quizAnswers",
            "#qid": str(question_id),
        },
        ExpressionAttributeValues={":ans": str(answer_id), ":t": now},
    )
    return now


def _next_question_id(current_qid: str, quiz_order: list[str]):
    if not quiz_order:
        return None
    if current_qid not in quiz_order:
        return quiz_order[0]
    idx = quiz_order.index(current_qid)
    return quiz_order[idx + 1] if idx + 1 < len(quiz_order) else None


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
    # ========== SQS MODE ==========
    # Triggered by StandProdQuizQueue; each record carries a quiz_start payload.
    if isinstance(event, dict) and "Records" in event:
        results = []
        for record in event["Records"]:
            try:
                inner = json.loads(record.get("body") or "{}")
                results.append(lambda_handler(inner, context))
            except Exception as e:
                log("sqs_record_error", {"error": repr(e)})
                results.append({"ok": False, "error": repr(e)})
        return {"ok": True, "processed": len(event["Records"]), "results": results}

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
                        "instagramPSID": it.get("instagramPSID"),
                        "instagramUsername": it.get("instagramUsername"),
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
                    "instagramPSID": it.get("instagramPSID"),
                    "instagramUsername": it.get("instagramUsername"),
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

    # ========== INTERNAL EVENT MODE ==========
    try:
        kind = (event or {}).get("kind")

        # ---- quiz_start ----
        if kind == "quiz_start":
            QUIZ_INTRO_TEXT = (
                "Antes de jugar, tienes que responder unas preguntas...\n"
            )

            game_id = (event.get("gameId") or event.get("game_id") or "").strip()
            raw_psids = event.get("psid") or []

            psids = [p.strip() for p in raw_psids if isinstance(p, str) and p.strip()]
            if not game_id or not psids:
                log("bad_quiz_start_event", event)
                return {"ok": False, "error": "missing_gameId_or_psids"}

            quiz_order, quiz_questions, meta = _get_quiz_meta(game_id)
            if not meta:
                for p in psids:
                    _send_dm(p, "Esa partida no existe. 🙈")
                return {"ok": False, "error": "game_not_found"}

            if not quiz_order:
                # IMPORTANT: do NOT send intro text here (no quiz)
                return {"ok": False, "error": "no_quiz_config"}

            game_type = (meta.get("gameType") or "").upper()
            first_qid = quiz_order[0]
            results = []

            for psid in psids:
                player = _get_player_by_psid_and_game(psid, game_id)
                if not player:
                    _send_dm(psid, "No he encontrado tu participación en la partida. ¿Has usado el código correcto?")
                    results.append({"psid": psid, "started": False, "reason": "player_not_found"})
                    continue

                pid = _as_int(player.get("playerId"))
                if pid is None:
                    results.append({"psid": psid, "started": False, "reason": "bad_playerId"})
                    continue

                # Send intro then first question in one batch so order is guaranteed
                first_q_msg = _build_quiz_question_message(psid, first_qid, quiz_questions)
                messages = [{"psid": psid, "text": QUIZ_INTRO_TEXT}]
                if first_q_msg:
                    messages.append(first_q_msg)
                _invoke_instagram_sender(messages)

                _set_quiz_state(game_id, pid, game_type, first_qid, completed=False)

                results.append({"psid": psid, "started": True, "playerId": pid, "firstQuestion": first_qid})

            return {"ok": True, "gameId": game_id, "results": results}


        # ---- quiz_answer ----
        if kind != "quiz_answer":
            log("ignored_event", {"kind": kind})
            return {"ok": True, "ignored": True}

        psid = event.get("psid")
        quiz_payload = (event.get("quizPayload") or "").strip()

        if not psid or not quiz_payload:
            log("bad_quiz_event", event)
            return {"ok": False, "error": "missing_psid_or_payload"}

        game_id, question_id, answer_id = _parse_quiz_payload(quiz_payload)

        # controles QR_*
        if game_id is None and question_id is None and answer_id and answer_id.startswith("QR_"):
            _send_dm(psid, "Cuestionario cerrado. Gracias igualmente 🙌")
            return {"ok": True, "closed": True}

        if not game_id or not question_id or not answer_id:
            return {"ok": False, "error": "invalid_quiz_payload"}

        quiz_order, quiz_questions, meta = _get_quiz_meta(game_id)
        if not meta:
            return {"ok": False, "error": "game_not_found"}

        game_type = (meta.get("gameType") or "").upper()
        if not game_type or game_type == "UNKNOWN":
            return {"ok": False, "error": "missing_gameType"}

        if not quiz_order:
            return {"ok": False, "error": "no_quiz_config"}

        player = _get_player_by_psid_and_game(psid, game_id)
        if not player:
            return {"ok": False, "error": "player_not_found"}

        pid = _as_int(player.get("playerId"))
        if pid is None:
            return {"ok": False, "error": "bad_playerId"}

        _save_quiz_answer(game_id, pid, game_type, question_id, answer_id)

        next_qid = _next_question_id(question_id, quiz_order)

        code = player.get("validationCode")
        if not next_qid:
            _set_quiz_state(game_id, pid, game_type, None, completed=True)

            if code:
                _send_dm(psid, f"🎟️ Tu código para jugar es: {code}\n\nVe a la pantalla, introdúcelo y ¡a jugar! 🚀")
            else:
                _send_dm(psid, "He tenido un problema generando tu código. Escribe 'AYUDA' y te lo soluciono.")

            return {"ok": True, "completed": True, "gameId": game_id, "playerId": pid}

        _set_quiz_state(game_id, pid, game_type, next_qid, completed=False)
        _send_quiz_question(psid, next_qid, quiz_questions)
        return {"ok": True, "nextQuestion": next_qid, "gameId": game_id, "playerId": pid}

    except ClientError as e:
        log("quiz_dynamo_error", {"error": str(e)})
        return {"ok": False, "error": "DynamoError", "detail": str(e)}
    except Exception as e:
        log("quiz_processor_error", {"error": repr(e)})
        return {"ok": False, "error": "internal_error", "message": str(e)}
