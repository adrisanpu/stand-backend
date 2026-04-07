import os
import json
import uuid
import boto3
from botocore.exceptions import ClientError
from stand_common.utils import log, _resp, _get_claims, _read_json_body, _iso_now

# ========= ENV VARS =========
GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
BEDROCK_REGION = os.environ.get("BEDROCK_REGION", "us-east-1")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "amazon.nova-lite-v1:0")

# ========= AWS =========
dynamodb = boto3.resource("dynamodb")
games_table = dynamodb.Table(GAMES_TABLE)
bedrock = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)

# ========= PROMPT =========

def _build_prompt(text: str, num_questions: int, num_options: int, use_emojis: bool) -> str:
    if use_emojis:
        emojis_instruction = (
            " Usa ÚNICAMENTE caracteres emoji Unicode reales. "
            "Pon un emoji al inicio del texto de cada pregunta y un emoji al inicio del título de cada opción de respuesta. "
            "Los emojis cuentan dentro del límite de 20 caracteres por opción."
        )
    else:
        emojis_instruction = ""
    option_ids = ["a", "b", "c", "d"][:num_options]
    options_json = ",\n        ".join(
        f'{{"answerId": "{oid}", "title": "Opción {oid.upper()}"}}' for oid in option_ids
    )
    return f"""Genera exactamente {num_questions} pregunta(s) de quiz de opción múltiple, cada una con {num_options} opciones de respuesta, siguiendo las indicaciones del prompt que aparece más abajo.

REGLAS CRÍTICAS PARA LAS OPCIONES DE RESPUESTA:
- Las opciones se mostrarán como quick replies en Instagram, con un máximo de 20 caracteres por botón. Ninguna opción puede superar 20 caracteres (incluyendo espacios y emoji).
- Escribe cada opción con 1, 2 o 3 palabras cortas. Ejemplos válidos: "Sales", "Glucosa", "En muñeca", "App móvil", "Sobre la piel".
- Cada opción debe ser una respuesta completa y autocontenida. Prohibido frases incompletas o cortadas (mal: "Directamente sobre la"; bien: "Sobre la piel" o "En la muñeca"). Si no cabe en 20 caracteres, usa una forma más breve.
- Es crítico que ninguna opción supere 20 caracteres. En caso de no cumplir, sustituye por una opción más corta.{emojis_instruction}

Indica la opción correcta en correctAnswerId.

Prompt: {text}

Responde ÚNICAMENTE con un JSON válido:
{{
  "quiz": [
    {{
      "text": "¿Pregunta?",
      "options": [
        {options_json}
      ],
      "correctAnswerId": "a"
    }}
  ]
}}"""


def _call_bedrock(prompt: str) -> str:
    response = bedrock.converse(
        modelId=BEDROCK_MODEL_ID,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={"maxTokens": 4096},
    )
    return response["output"]["message"]["content"][0]["text"]


def _parse_json_response(text: str) -> dict:
    """Extract and parse the first JSON object found in text."""
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError("No JSON object found in model response")
    return json.loads(text[start:end + 1])


MAX_OPTION_TITLE_LENGTH = 20


def _normalize_quiz(raw_quiz: list, game_id: str):
    """Returns (quiz_order, quiz_questions, questions_response) or raises."""
    quiz_order = []
    quiz_questions = {}
    questions_response = []

    for q in raw_quiz:
        q_id = str(uuid.uuid4())
        text = (q.get("text") or "").strip()
        options = q.get("options") or []
        correct = q.get("correctAnswerId") or ""

        if not text or not options:
            continue

        def _short_title(s: str) -> str:
            t = (s or "").strip()
            return t[:MAX_OPTION_TITLE_LENGTH] if len(t) > MAX_OPTION_TITLE_LENGTH else t

        normalized_options = [
            {
                "title": _short_title(opt.get("title", "")),
                "payload": f"{game_id}_{q_id}_{opt.get('answerId', '')}",
                "answerId": opt.get("answerId", ""),
            }
            for opt in options
        ]

        quiz_order.append(q_id)
        quiz_questions[q_id] = {
            "text": text,
            "correctAnswerId": correct,
            "options": normalized_options,
        }
        questions_response.append({
            "id": q_id,
            "text": text,
            "options": normalized_options,
        })

    return quiz_order, quiz_questions, questions_response


def _handle_infocards_invoke(event: dict):
    """Handle direct Lambda invocation from infocards process. Event has source, gameId, text, ownerUserId."""
    try:
        game_id = (event.get("gameId") or "").strip()
        text = (event.get("text") or "").strip()
        owner_user_id = (event.get("ownerUserId") or "").strip()

        if not game_id:
            return _resp(400, {"error": "Missing required field 'gameId'."})
        if not text:
            return _resp(400, {"error": "Missing required field 'text'."})
        if not owner_user_id:
            return _resp(400, {"error": "Missing required field 'ownerUserId'."})

        num_questions = 3
        if "numQuestions" in event:
            try:
                n = int(event["numQuestions"])
                num_questions = max(1, min(5, n))
            except (TypeError, ValueError):
                pass

        num_options = 3
        if "numOptions" in event:
            try:
                n = int(event["numOptions"])
                num_options = max(2, min(4, n))
            except (TypeError, ValueError):
                pass

        use_emojis = bool(event.get("useEmojis", True))

        try:
            resp = games_table.get_item(Key={"gameId": game_id})
            item = resp.get("Item")
        except ClientError as e:
            log("GetItem failed", {"error": str(e)})
            return _resp(500, {"error": "GetItemFailed", "detail": str(e)})

        if not item:
            return _resp(404, {"error": "GameNotFound", "gameId": game_id})

        if (item.get("ownerUserId") or "") != owner_user_id:
            return _resp(403, {"error": "Forbidden", "detail": "Not owner of this game."})

        prompt = _build_prompt(text, num_questions, num_options, use_emojis)
        log("Calling Bedrock (infocards invoke)", {"gameId": game_id, "modelId": BEDROCK_MODEL_ID})

        try:
            raw_text = _call_bedrock(prompt)
        except Exception as e:
            log("Bedrock call failed", {"error": str(e)})
            return _resp(502, {"error": "BedrockCallFailed", "detail": str(e)})

        try:
            parsed = _parse_json_response(raw_text)
        except Exception as e:
            log("JSON parse failed", {"error": str(e), "rawText": raw_text[:500]})
            return _resp(502, {"error": "ResponseParseError", "detail": str(e)})

        raw_quiz = parsed.get("quiz") or []
        if not raw_quiz:
            return _resp(502, {"error": "NoQuestionsGenerated", "detail": "Model returned no quiz questions."})

        quiz_order, quiz_questions, questions_response = _normalize_quiz(raw_quiz, game_id)
        if not quiz_order:
            return _resp(502, {"error": "NoQuestionsGenerated", "detail": "All generated questions were invalid."})

        now = _iso_now()
        try:
            games_table.update_item(
                Key={"gameId": game_id},
                UpdateExpression="SET quizOrder = :o, quizQuestions = :q, updatedAt = :now",
                ExpressionAttributeValues={
                    ":o": quiz_order,
                    ":q": quiz_questions,
                    ":now": now,
                },
                ConditionExpression="attribute_exists(gameId)",
            )
        except ClientError as e:
            log("UpdateItem failed", {"error": str(e)})
            return _resp(500, {"error": "UpdateItemFailed", "detail": str(e)})

        return _resp(200, {
            "ok": True,
            "gameId": game_id,
            "questions": questions_response,
        })
    except Exception as e:
        log("UnhandledError (infocards invoke)", {"error": str(e)})
        return _resp(500, {"error": "UnhandledError", "detail": str(e)})


def lambda_handler(event, context):
    # Direct invocation from infocards process (no HTTP)
    if event.get("source") == "infocards_process":
        return _handle_infocards_invoke(event)

    method = (
        event.get("httpMethod")
        or event.get("requestContext", {}).get("http", {}).get("method", "POST")
    )

    if method == "OPTIONS":
        return _resp(204, "")

    if method != "POST":
        return _resp(405, {"error": f"Method {method} not allowed"})

    try:
        claims = _get_claims(event)
        owner_user_id = (claims.get("sub") or "").strip()
        if not owner_user_id:
            return _resp(401, {"error": "Missing sub claim (unauthorized)"})

        body = _read_json_body(event)
        if body is None:
            return _resp(400, {"error": "Invalid JSON body"})

        game_id = (body.get("gameId") or "").strip()
        text = (body.get("text") or "").strip()

        if not game_id:
            return _resp(400, {"error": "Missing required field 'gameId'."})

        if not text:
            return _resp(400, {"error": "Missing required field 'text'."})

        num_questions = 3
        if "numQuestions" in body:
            try:
                n = int(body["numQuestions"])
                num_questions = max(1, min(5, n))
            except (TypeError, ValueError):
                pass

        num_options = 3
        if "numOptions" in body:
            try:
                n = int(body["numOptions"])
                num_options = max(2, min(4, n))
            except (TypeError, ValueError):
                pass

        use_emojis = bool(body.get("useEmojis", False))

        # Verify game ownership
        try:
            resp = games_table.get_item(Key={"gameId": game_id})
            item = resp.get("Item")
        except ClientError as e:
            log("GetItem failed", {"error": str(e)})
            return _resp(500, {"error": "GetItemFailed", "detail": str(e)})

        if not item:
            return _resp(404, {"error": "GameNotFound", "gameId": game_id})

        if (item.get("ownerUserId") or "") != owner_user_id:
            return _resp(403, {"error": "Forbidden", "detail": "Not owner of this game."})

        # Build prompt and call Bedrock
        prompt = _build_prompt(text, num_questions, num_options, use_emojis)
        log("Calling Bedrock", {"gameId": game_id, "modelId": BEDROCK_MODEL_ID, "numQuestions": num_questions, "numOptions": num_options, "useEmojis": use_emojis})

        try:
            raw_text = _call_bedrock(prompt)
        except Exception as e:
            log("Bedrock call failed", {"error": str(e)})
            return _resp(502, {"error": "BedrockCallFailed", "detail": str(e)})

        # Parse response
        try:
            parsed = _parse_json_response(raw_text)
        except Exception as e:
            log("JSON parse failed", {"error": str(e), "rawText": raw_text[:500]})
            return _resp(502, {"error": "ResponseParseError", "detail": str(e)})

        raw_quiz = parsed.get("quiz") or []
        if not raw_quiz:
            return _resp(502, {"error": "NoQuestionsGenerated", "detail": "Model returned no quiz questions."})

        quiz_order, quiz_questions, questions_response = _normalize_quiz(raw_quiz, game_id)

        if not quiz_order:
            return _resp(502, {"error": "NoQuestionsGenerated", "detail": "All generated questions were invalid."})

        # Save to DynamoDB
        now = _iso_now()
        try:
            games_table.update_item(
                Key={"gameId": game_id},
                UpdateExpression="SET quizOrder = :o, quizQuestions = :q, updatedAt = :now",
                ExpressionAttributeValues={
                    ":o": quiz_order,
                    ":q": quiz_questions,
                    ":now": now,
                },
                ConditionExpression="attribute_exists(gameId)",
            )
        except ClientError as e:
            log("UpdateItem failed", {"error": str(e)})
            return _resp(500, {"error": "UpdateItemFailed", "detail": str(e)})

        return _resp(200, {
            "ok": True,
            "gameId": game_id,
            "questions": questions_response,
        })

    except Exception as e:
        log("UnhandledError", {"error": str(e)})
        return _resp(500, {"error": "UnhandledError", "detail": str(e)})
