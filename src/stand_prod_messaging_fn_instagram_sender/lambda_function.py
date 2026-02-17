import os
import json
import urllib.request
import urllib.parse
import urllib.error
import boto3

# --- IG ENV ---
IG_GRAPH_VERSION = os.environ.get("IG_GRAPH_VERSION", "v24.0")
IG_TIMEOUT       = int(os.environ.get("IG_TIMEOUT_SECONDS", "8"))

# Instagram keys solo desde Secrets Manager (INSTAGRAM_SECRET_NAME obligatorio)
# Secret: PAGE_TOKEN, VERIFY_TOKEN, SENDER_ID
IG_PAGE_TOKEN = ""
IG_SENDER_ID  = ""
_instagram_secret_name = os.environ.get("INSTAGRAM_SECRET_NAME", "").strip()
if _instagram_secret_name:
    try:
        sm = boto3.client("secretsmanager")
        raw = sm.get_secret_value(SecretId=_instagram_secret_name)
        data = json.loads(raw.get("SecretString", "{}"))
        if data:
            IG_PAGE_TOKEN = (data.get("PAGE_TOKEN") or data.get("IG_PAGE_TOKEN") or "").strip()
            IG_SENDER_ID  = (data.get("SENDER_ID") or data.get("IG_SENDER_ID") or "").strip()
    except Exception as e:
        print(json.dumps({"msg": "instagram_secret_load_failed", "error": repr(e)}))


def log(msg, obj=None):
    if obj is not None:
        print(json.dumps({"msg": msg, "data": obj}, ensure_ascii=False))
    else:
        print(json.dumps({"msg": msg}, ensure_ascii=False))


def _graph_post(path: str, payload: dict):
    """
    Low-level POST to Instagram Graph API.
    Returns: (ok: bool, status: int|None, resp_text: str|None)
    """
    if not (IG_SENDER_ID and IG_PAGE_TOKEN):
        log("graph_missing_config", {"has_sender_id": bool(IG_SENDER_ID), "has_page_token": bool(IG_PAGE_TOKEN)})
        return False, None, None

    base = f"https://graph.facebook.com/{IG_GRAPH_VERSION}/{path}"
    url  = f"{base}?access_token={urllib.parse.quote(IG_PAGE_TOKEN)}"

    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=IG_TIMEOUT) as r:
            status = getattr(r, "status", 200)
            raw = r.read().decode("utf-8") if r else ""
            return True, status, raw

    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8")
        except Exception:
            err_body = "<no body>"
        log("graph_http_error", {"status": e.code, "path": path, "err_body": err_body[:500]})
        return False, e.code, err_body

    except Exception as e:
        log("graph_error", {"error": repr(e), "path": path})
        return False, None, None


def _normalize_quick_replies(quick_replies):
    """
    Acepta:
      A) Ya en formato IG:
         [{"content_type":"text","title":"...","payload":"..."}]
      B) Formato simple:
         [{"title":"...","payload":"..."}]
    Devuelve lista en formato IG o None
    """
    if not quick_replies or not isinstance(quick_replies, list):
        return None

    out = []
    for qr in quick_replies:
        if not isinstance(qr, dict):
            continue
        title = (qr.get("title") or "").strip()
        payload = (qr.get("payload") or "").strip()
        content_type = (qr.get("content_type") or "text").strip()

        if not title or not payload:
            continue

        out.append({
            "content_type": content_type,
            "title": title,
            "payload": payload
        })

    return out or None


def _send_single_message(psid: str, text: str = None, quick_replies=None, image_url: str = None) -> dict:
    """
    Send a single message:
      - text (with optional quick replies), OR
      - image_url (image attachment)

    Returns dict result: {ok, status?, error?, preview?}
    """
    if not psid or psid == "#":
        return {"ok": False, "error": "invalid_psid"}

    if not text and not image_url:
        return {"ok": False, "error": "missing_content"}

    # Si hay image_url, enviamos imagen; si no, texto.
    if image_url:
        message = {
            "attachment": {
                "type": "image",
                "payload": {
                    "url": image_url,
                    "is_reusable": False
                }
            }
        }
        preview = image_url[:120]
    else:
        message = {"text": text}
        qr_norm = _normalize_quick_replies(quick_replies)
        if qr_norm:
            message["quick_replies"] = qr_norm
        preview = (text or "")[:120]

    payload = {
        "recipient": {"id": psid},
        "messaging_type": "RESPONSE",
        "message": message
    }

    ok, status, resp_text = _graph_post(f"{IG_SENDER_ID}/messages", payload)

    if not ok:
        log("dm_send_failed", {"psid": psid, "status": status, "preview": preview})
        return {"ok": False, "status": status, "error": "send_failed", "preview": preview}

    return {"ok": True, "status": status}


def lambda_handler(event, context):
    """
    Invocación interna:
    {
      "messages": [
        { "psid": "123", "text": "Hola" },
        { "psid": "456", "text": "Pregunta", "quick_replies": [ ... ] },
        { "psid": "789", "image_url": "https://..." }
      ]
    }
    """
    try:
        body = event or {}
        messages = body.get("messages", [])

        if not isinstance(messages, list):
            log("invalid_messages_payload", {"type": str(type(messages))})
            return {"ok": False, "error": "InvalidMessagesPayload", "total": 0, "success": 0, "failed": 0, "results": []}

        results = []
        success = 0
        failed = 0

        for msg in messages:
            if not isinstance(msg, dict):
                failed += 1
                results.append({"ok": False, "reason": "message_not_object"})
                continue

            psid = msg.get("psid")
            text = msg.get("text")
            quick_replies = msg.get("quick_replies")
            image_url = msg.get("image_url")

            # Permitimos text O image_url (al menos uno)
            if not psid or (not text and not image_url):
                log("skip_invalid_message", {"message": msg})
                failed += 1
                results.append({"psid": psid, "ok": False, "reason": "missing_psid_or_content"})
                continue

            r = _send_single_message(psid, text=text, quick_replies=quick_replies, image_url=image_url)
            if r.get("ok"):
                success += 1
            else:
                failed += 1
            results.append({"psid": psid, **r})

        log("dm_send_done", {"total": len(messages), "success": success, "failed": failed})
        return {"ok": True, "total": len(messages), "success": success, "failed": failed, "results": results}

    except Exception as e:
        log("instagram_sender_internal_error", {"error": repr(e)})
        return {"ok": False, "error": "internal_error", "message": "Error interno enviando mensajes de Instagram."}
