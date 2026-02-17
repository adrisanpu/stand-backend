import os, json, base64, urllib.parse, urllib.request, re
import boto3

# ====== ENV VARS ======
IG_GRAPH_VERSION = os.environ.get("IG_GRAPH_VERSION", "v24.0")
ASSIGN_LAMBDA = os.environ.get("ASSIGN_LAMBDA", "stand-prod-game-fn-assign")
QUIZ_LAMBDA   = os.environ.get("QUIZ_LAMBDA", "stand-prod-game-fn-quizz")

# Instagram keys solo desde Secrets Manager (INSTAGRAM_SECRET_NAME obligatorio)
# Secret: PAGE_TOKEN, VERIFY_TOKEN, SENDER_ID
IG_PAGE_TOKEN = ""
VERIFY_TOKEN  = ""
IG_SENDER_ID  = ""
_instagram_secret_name = os.environ.get("INSTAGRAM_SECRET_NAME", "")

if _instagram_secret_name:
    try:
        sm = boto3.client("secretsmanager")
        raw = sm.get_secret_value(SecretId=_instagram_secret_name)
        data = json.loads(raw.get("SecretString", "{}"))
        if data:
            IG_PAGE_TOKEN = data.get("PAGE_TOKEN") or ""
            VERIFY_TOKEN  = data.get("VERIFY_TOKEN") or ""
            IG_SENDER_ID  = data.get("SENDER_ID") or ""
    except Exception as e:
        print(json.dumps({"msg": "instagram_secret_load_failed", "error": repr(e)}))
# ======================

print("instagram_secret_loaded", {"keys": list(data.keys()), "has_page_token": bool(IG_PAGE_TOKEN)})

lambda_client = boto3.client("lambda")

JSON_HEADERS = {"Content-Type": "application/json"}


def log(msg, obj=None):
    if obj is not None:
        print(json.dumps({"msg": msg, "data": obj}, ensure_ascii=False))
    else:
        print(json.dumps({"msg": msg}, ensure_ascii=False))


def _ok(status=200, body=None, headers=None):
    h = dict(JSON_HEADERS)
    if headers:
        h.update(headers)
    return {"statusCode": int(status), "headers": h, "body": json.dumps(body or {"ok": True}, ensure_ascii=False)}


# ---------- Instagram Graph helpers ----------

def _graph_get(path: str, params: dict):
    if not IG_PAGE_TOKEN:
        log("graph_skip_get", {"reason": "missing_page_token"})
        return None

    base = f"https://graph.facebook.com/{IG_GRAPH_VERSION}/{path}"
    q = params.copy()
    q["access_token"] = IG_PAGE_TOKEN
    url = base + "?" + urllib.parse.urlencode(q)

    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=8) as r:
            raw_resp = r.read().decode("utf-8")
            return json.loads(raw_resp)
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8")
        except Exception:
            err_body = "<no body>"
        log("graph_get_http_error", {"status": e.code, "url": url, "err_body": err_body})
        return None
    except Exception as e:
        log("graph_get_error", {"error": repr(e), "url": url})
        return None


def _get_username_from_graph(psid: str):
    if not psid:
        return None

    data = _graph_get(psid, {"fields": "username"})
    if not data:
        return None

    uname = (data.get("username") or "").strip()
    if not uname:
        return None

    # normaliza: empieza con @ y en minúsculas
    uname = uname.lower()
    if not uname.startswith("@"):
        uname = "@" + uname
    return uname


# ---------- Clasificación de mensajes ----------

def _get_quick_payload(m: dict):
    """Devuelve payload de quick reply o postback, si existe."""
    msg = m.get("message") or {}
    qr = (msg.get("quick_reply") or {}).get("payload")
    if qr:
        return qr

    postback = m.get("postback") or {}
    pb = postback.get("payload")
    if pb:
        return pb

    return None


# gameId nuevo: 6 dígitos numéricos (string, permite ceros iniciales)
_GAMEID_RE = re.compile(r"^\d{6}$")

def _looks_like_game_id(text: str) -> bool:
    if not text:
        return False
    t = text.strip()
    return bool(_GAMEID_RE.fullmatch(t))


def _classify_message(m: dict):
    """
    Devuelve (kind, data):
      - ("quiz_answer", payload)
      - ("game_id", game_id_str)
      - ("other", None)
    """
    msg = m.get("message") or {}
    text = (msg.get("text") or "").strip()

    quick_payload = _get_quick_payload(m)
    if quick_payload:
        return "quiz_answer", quick_payload

    if _looks_like_game_id(text):
        return "game_id", text.strip().upper()

    return "other", None


# ---------- Dispatcher ----------

def _invoke_async(function_name: str, payload: dict):
    try:
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="Event",
            Payload=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        )
        return True
    except Exception as e:
        log("invoke_error", {"error": repr(e), "target": function_name})
        return False


def _handle_and_dispatch(m: dict):
    psid = (m.get("sender") or {}).get("id")
    msg  = m.get("message") or {}

    if not psid:
        log("missing_psid_in_message", m)
        return

    # 🚫 Ignorar mensajes enviados por tu propia cuenta (echo)
    if (IG_SENDER_ID and psid == IG_SENDER_ID) or msg.get("is_echo"):
        log("ignored_echo_or_self_message", {"psid": psid})
        return

    kind, data = _classify_message(m)

    if kind == "other":
        return

    if kind == "quiz_answer":
        normalized_event = {
            "kind": "quiz_answer",
            "psid": psid,
            "quizPayload": data,
            "raw_event": m,
        }
        log("dispatching_quiz_answer", {"psid": psid, "quizPayload": str(data)[:120]})
        _invoke_async(QUIZ_LAMBDA, normalized_event)
        return

    if kind == "game_id":
        game_id = data
        username_at = _get_username_from_graph(psid)

        normalized_event = {
            "kind": "game_id",
            "psid": psid,
            "username_at": username_at,  # puede ser None
            "game_id": game_id,
            "raw_event": m,
        }
        log("dispatching_game_id", {"psid": psid, "game_id": game_id, "username_at": username_at})
        _invoke_async(ASSIGN_LAMBDA, normalized_event)
        return

    log("unhandled_kind", {"kind": kind})


# ---------- Lambda handler ----------

def lambda_handler(event, context):
    method = (
        event.get("httpMethod")
        or event.get("requestContext", {}).get("http", {}).get("method")
        or ""
    ).upper()

    # No necesitas CORS, pero OPTIONS puede venir: respondemos 204
    if method == "OPTIONS":
        return {"statusCode": 204, "body": ""}

    # GET: verificación de webhook
    if method == "GET":
        qs = event.get("queryStringParameters") or {}
        challenge = qs.get("hub.challenge") or qs.get("hub.challenge".lower())
        verify    = qs.get("hub.verify_token") or qs.get("hub.verify_token".lower())

        if not challenge:
            log("webhook_verify_missing_challenge", {"qs": qs})
            return {"statusCode": 400, "headers": JSON_HEADERS, "body": "missing challenge"}

        if VERIFY_TOKEN and verify != VERIFY_TOKEN:
            log("webhook_bad_verify_token", {"provided": verify})
            return {"statusCode": 403, "headers": JSON_HEADERS, "body": "bad verify token"}

        log("webhook_verified")
        # Instagram/Facebook espera texto plano con el challenge
        return {"statusCode": 200, "headers": {"Content-Type": "text/plain"}, "body": str(challenge)}

    # POST: eventos IG
    try:
        raw = event.get("body") or ""
        if event.get("isBase64Encoded"):
            try:
                raw = base64.b64decode(raw).decode("utf-8")
            except Exception as e:
                log("b64_decode_error", {"error": repr(e)})
                raw = ""

        body = json.loads(raw or "{}")
        entries = body.get("entry") or []

        # Recorremos mensajes
        count = 0
        for entry in entries:
            for m in (entry.get("messaging") or []):
                count += 1
                _handle_and_dispatch(m)

        return _ok(200, {"received": True, "events": count})

    except Exception as e:
        log("webhook_error", {"error": repr(e)})
        # Facebook recomienda responder 200 para no reintentar en bucle si ya logueamos
        return _ok(200, {"received": False})
