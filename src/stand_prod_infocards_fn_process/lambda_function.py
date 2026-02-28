import os
import json
import uuid
import boto3
from botocore.exceptions import ClientError
from stand_common.utils import log, _resp, _get_claims, _read_json_body, _iso_now, set_game_type_blob

# ========= ENV VARS =========
GAMES_TABLE = os.environ.get("GAMES_TABLE", "stand-prod-game-table")
INFOCARDS_BUCKET = os.environ.get("INFOCARDS_BUCKET", "")
BEDROCK_REGION = os.environ.get("BEDROCK_REGION", "us-east-1")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "amazon.nova-lite-v1:0")
QUIZ_GENERATE_FUNCTION_NAME = os.environ.get("QUIZ_GENERATE_FUNCTION_NAME", "stand-prod-quiz-fn-generate")

# ========= AWS =========
dynamodb = boto3.resource("dynamodb")
games_table = dynamodb.Table(GAMES_TABLE)
s3 = boto3.client("s3")
bedrock = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)
lambda_client = boto3.client("lambda")

# ========= PROMPTS =========

PROMPT_CARDS_ONLY = """Eres un experto en marketing y comunicación de producto.
A partir del siguiente contenido, genera exactamente {num_cards} tarjetas informativas para mostrar en un stand de feria.
Cada tarjeta debe tener un título corto y llamativo y un cuerpo explicativo de 2-4 frases.

Contenido:
{content}

Responde ÚNICAMENTE con un JSON válido con este formato (sin texto adicional):
{{
  "cards": [
    {{
      "title": "Título de la tarjeta",
      "body": "Descripción detallada del punto clave."
    }}
  ]
}}"""


def _extract_pdf_text(bucket: str, key: str) -> str:
    """Download a PDF from S3 and attempt basic text extraction."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        # Attempt naive text extraction from PDF bytes (works for simple PDFs)
        text = data.decode("latin-1", errors="ignore")
        # Keep only printable ASCII-ish characters
        lines = [line.strip() for line in text.splitlines() if len(line.strip()) > 3]
        return "\n".join(lines[:300])  # cap to avoid huge prompts
    except Exception as e:
        log("PDF extraction failed", {"key": key, "error": str(e)})
        return f"[No se pudo extraer el texto del PDF: {key}]"


def _build_content(text: str, file_keys: list) -> str:
    parts = []

    if text:
        parts.append(f"Descripción proporcionada:\n{text}")

    pdf_keys = [k for k in file_keys if k.lower().endswith(".pdf")]

    for key in pdf_keys:
        if INFOCARDS_BUCKET:
            pdf_text = _extract_pdf_text(INFOCARDS_BUCKET, key)
            parts.append(f"Contenido del PDF ({key.split('/')[-1]}):\n{pdf_text}")

    return "\n\n".join(parts)


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


def _normalize_cards(raw_cards: list, game_id: str) -> list:
    normalized = []
    for idx, card in enumerate(raw_cards):
        normalized.append({
            "id": str(uuid.uuid4()),
            "title": (card.get("title") or "").strip(),
            "body": (card.get("body") or "").strip(),
            "imageKey": None,
            "videoKey": None,
            "order": idx,
        })
    return normalized


def _invoke_quiz_generate(
    game_id: str,
    content: str,
    owner_user_id: str,
    *,
    num_questions: int = 3,
    num_options: int = 3,
    use_emojis: bool = False,
) -> bool:
    """Invoke the quiz-generate Lambda with the same content. Returns True if quiz was generated successfully."""
    payload = json.dumps({
        "source": "infocards_process",
        "gameId": game_id,
        "text": content,
        "ownerUserId": owner_user_id,
        "numQuestions": num_questions,
        "numOptions": num_options,
        "useEmojis": use_emojis,
    })
    try:
        response = lambda_client.invoke(
            FunctionName=QUIZ_GENERATE_FUNCTION_NAME,
            InvocationType="RequestResponse",
            Payload=payload,
        )
        payload_out = response.get("Payload")
        if payload_out is None:
            log("Quiz invoke: no payload in response", {"gameId": game_id})
            return False
        result = json.loads(payload_out.read().decode("utf-8"))
        status = result.get("statusCode", 0)
        if status != 200:
            body = result.get("body", "")
            log("Quiz invoke returned error", {"gameId": game_id, "statusCode": status, "body": body[:500]})
            return False
        return True
    except Exception as e:
        log("Quiz invoke failed", {"gameId": game_id, "error": str(e)})
        return False


def lambda_handler(event, context):
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
        file_keys = body.get("fileKeys") or []
        generate_quiz = bool(body.get("generateQuiz", False))

        num_cards = 6
        if "numCards" in body:
            try:
                n = int(body["numCards"])
                num_cards = max(3, min(10, n))
            except (TypeError, ValueError):
                pass

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

        if not game_id:
            return _resp(400, {"error": "Missing required field 'gameId'."})

        if not text and not file_keys:
            return _resp(400, {"error": "Provide at least 'text' or 'fileKeys'."})

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

        # Build content and prompt (cards only)
        content = _build_content(text, file_keys)
        prompt = PROMPT_CARDS_ONLY.format(content=content, num_cards=num_cards)

        # Call Bedrock
        log("Calling Bedrock", {"gameId": game_id, "numCards": num_cards, "generateQuiz": generate_quiz, "modelId": BEDROCK_MODEL_ID})
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

        raw_cards = parsed.get("cards") or []

        if not raw_cards:
            return _resp(502, {"error": "NoCardsGenerated", "detail": "Model returned no cards."})

        cards = _normalize_cards(raw_cards, game_id)
        now = _iso_now()

        try:
            set_game_type_blob(
                games_table,
                game_id,
                "INFOCARDS",
                {"cards": cards},
                updated_at=now,
                condition_expression="attribute_exists(gameId)",
            )
        except ClientError as e:
            log("UpdateItem failed", {"error": str(e)})
            return _resp(500, {"error": "UpdateItemFailed", "detail": str(e)})

        quiz_generated = False
        if generate_quiz:
            quiz_generated = _invoke_quiz_generate(
                game_id, content, owner_user_id,
                num_questions=num_questions,
                num_options=num_options,
                use_emojis=use_emojis,
            )

        return _resp(200, {
            "ok": True,
            "gameId": game_id,
            "cards": cards,
            "quizGenerated": quiz_generated,
        })

    except Exception as e:
        log("UnhandledError", {"error": str(e)})
        return _resp(500, {"error": "UnhandledError", "detail": str(e)})
