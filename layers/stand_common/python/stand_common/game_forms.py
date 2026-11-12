"""Per-event presentation and a single, durable participant survey."""
import hashlib
import json
import os
import re
import uuid

from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from stand_common.utils import _iso_now, _json_sanitize, get_game_type_blob, normalize_participant_fields

MOMENTS = {"disabled", "after_registration", "after_game", "prize", "raffle"}
DEFAULT = {
    "registrationTitle": "", "registrationDescription": "",
    "quizTitle": "", "quizDescription": "", "logoKey": None, "bannerKey": None,
    "appearance": None, "quizMoment": "disabled",
}


class FormError(Exception):
    def __init__(self, code, status=409):
        super().__init__(code)
        self.code, self.status = code, status


def config(meta):
    return {**DEFAULT, **(meta.get("formConfig") or {})}


def moment(meta):
    return config(meta)["quizMoment"]


def completed(meta, player):
    return bool(get_game_type_blob(player, meta["gameType"]).get("quizCompleted"))


def finished(meta, player):
    blob = get_game_type_blob(player, meta["gameType"])
    gt = meta["gameType"]
    if gt == "STACK":
        return blob.get("attempt", {}).get("status") == "finished"
    if gt == "RULET4":
        return blob.get("result") is not None
    if gt == "T1MER":
        return blob.get("score") is not None
    if gt == "IMPOSTOR":
        state = (meta.get("type") or {}).get("IMPOSTOR") or {}
        return state.get("phase") == "finished"
    return bool(player.get("validated"))


def pending(meta, player, at):
    return moment(meta) == at and not completed(meta, player)


def require(meta, player, at):
    if pending(meta, player, at):
        raise FormError("QuizRequired")


def questions(meta):
    return [{"id": qid, "text": meta["quizQuestions"][qid]["text"],
             "options": [{"title": opt["title"], "payload": opt["payload"]}
                         for opt in meta["quizQuestions"][qid].get("options", [])]}
            for qid in meta.get("quizOrder", [])
            if qid in (meta.get("quizQuestions") or {})]


def revision(meta):
    value = [config(meta), questions(meta), meta.get("participantFields")]
    return hashlib.sha256(json.dumps(_json_sanitize(value), sort_keys=True).encode()).hexdigest()


def players(dynamo, game_id):
    table = dynamo.Table(os.environ["GAMEPLAYER_TABLE"])
    args = {"KeyConditionExpression": Key("gameId").eq(game_id), "ConsistentRead": True}
    while True:
        page = table.query(**args)
        yield from page.get("Items", [])
        if not page.get("LastEvaluatedKey"):
            return
        args["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def locked(dynamo, meta):
    return bool(meta.get("quizLockedAt")) or any(
        completed(meta, p) or get_game_type_blob(p, meta["gameType"]).get("quizAnswers")
        for p in players(dynamo, meta["gameId"])
    )


def image_url(key):
    if not key:
        return None
    bucket = os.environ.get("USER_MEDIA_BUCKET")
    if not bucket:
        raise FormError("FormMediaUnavailable", 503)
    import boto3
    return boto3.client("s3").generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600,
    )


def public_config(meta):
    value = config(meta)
    return {"formConfig": value, "formLogoUrl": image_url(value["logoKey"]),
            "formBannerUrl": image_url(value["bannerKey"])}


def operator_shape(dynamo, meta):
    value = config(meta)
    from stand_common import kivly_registration
    loyalty = kivly_registration.game_settings(kivly_registration.apply_account_settings(dynamo, meta))
    return {
        "ok": True, "gameId": meta["gameId"], "gameType": meta["gameType"],
        "formConfig": value, "logoUrl": image_url(value["logoKey"]),
        "bannerUrl": image_url(value["bannerKey"]),
        "participantFields": normalize_participant_fields(meta["gameType"], meta.get("participantFields")),
        "questions": questions(meta), "locked": locked(dynamo, meta), "revision": revision(meta),
        "joinUrl": "/play/" + meta["gameId"],
        **({"loyalty": {key: loyalty[key] for key in ("clubName", "privacyUrl", "termsUrl", "consentVersion")}}
           if loyalty["enabled"] else {}),
        "readOnly": not meta.get("isActive", True) or bool(meta.get("archivedAt"))
                    or meta.get("lifecycleState", "active") != "active",
    }


def validate_config(raw, owner):
    if not isinstance(raw, dict) or set(raw) - set(DEFAULT):
        raise FormError("InvalidFormConfig", 400)
    value = {**DEFAULT, **raw}
    for key in ("registrationTitle", "registrationDescription", "quizTitle", "quizDescription"):
        text = value[key]
        if not isinstance(text, str) or len(text) > (160 if key.endswith("Title") else 1000):
            raise FormError("InvalidFormText", 400)
        value[key] = text.strip()
    if not isinstance(value["quizMoment"], str) or value["quizMoment"] not in MOMENTS:
        raise FormError("InvalidQuizMoment", 400)
    for key in ("logoKey", "bannerKey"):
        media = value[key]
        if media is not None and (not isinstance(media, str) or not media.startswith(f"users/{owner}/logos/")
                                  or ".." in media or len(media) > 1024):
            raise FormError("InvalidFormMedia", 400)
    appearance = value["appearance"]
    if appearance is not None:
        if not isinstance(appearance, dict) or set(appearance) != {"colors", "themeMode"}:
            raise FormError("InvalidFormAppearance", 400)
        colors = appearance["colors"]
        if appearance["themeMode"] not in ("light", "dark") or (colors is not None and (
            not isinstance(colors, dict) or set(colors) != {"primary", "accent"}
            or any(not isinstance(c, str) or not re.fullmatch(r"#[0-9a-fA-F]{6}", c) for c in colors.values())
        )):
            raise FormError("InvalidFormAppearance", 400)
    return value


def normalize_questions(raw, game_id):
    if not isinstance(raw, list) or len(raw) > 5:
        raise FormError("InvalidQuizQuestions", 400)
    result, seen = [], set()
    for q in raw:
        if not isinstance(q, dict):
            raise FormError("InvalidQuizQuestions", 400)
        qid, text, options = q.get("id"), q.get("text"), q.get("options")
        if (not isinstance(qid, str) or not re.fullmatch(r"[a-zA-Z0-9-]{1,64}", qid) or qid in seen
                or not isinstance(text, str) or not text.strip() or len(text) > 500
                or not isinstance(options, list) or not 1 <= len(options) <= 3):
            raise FormError("InvalidQuizQuestions", 400)
        seen.add(qid)
        opts = []
        for i, opt in enumerate(options):
            title = opt.get("title") if isinstance(opt, dict) else None
            if not isinstance(title, str) or not title.strip() or len(title) > 200:
                raise FormError("InvalidQuizOptions", 400)
            opts.append({"title": title.strip(), "payload": f"{game_id}_{qid}_{'abc'[i]}"})
        result.append({"id": qid, "text": text.strip(), "options": opts})
    return result


def save(dynamo, meta, body):
    if not isinstance(body, dict) or set(body) != {"formConfig", "participantFields", "questions", "expectedRevision"}:
        raise FormError("InvalidFormUpdate", 400)
    if body["expectedRevision"] != revision(meta):
        raise FormError("FormChanged")
    value = validate_config(body["formConfig"], meta["ownerUserId"])
    qs = questions(meta) if body["questions"] == questions(meta) else normalize_questions(body["questions"], meta["gameId"])
    if value["quizMoment"] != "disabled" and not qs:
        raise FormError("QuizQuestionsRequired", 400)
    if value["quizMoment"] == "after_game" and meta["gameType"] in ("INFOCARDS", "IMPOSTOR"):
        raise FormError("QuizMomentUnavailable", 400)
    try:
        fields = normalize_participant_fields(meta["gameType"], body["participantFields"], strict=True)
    except ValueError as exc:
        raise FormError(str(exc), 400) from None
    fields["email"] = {"required": True}
    from stand_common import kivly_registration
    resolved = kivly_registration.apply_account_settings(dynamo, meta)
    if kivly_registration.game_settings(resolved)["enabled"] and not fields["phone"].get("enabled", fields["phone"]["required"]):
        raise FormError("KivlyPhoneRequired", 400)
    structural = qs != questions(meta) or value["quizMoment"] != moment(meta)
    if structural and locked(dynamo, meta):
        raise FormError("QuizLocked")
    names = {"#form": "formConfig"}
    values = {":form": value, ":fields": fields, ":order": [q["id"] for q in qs],
              ":questions": {q["id"]: {"text": q["text"], "options": q["options"]} for q in qs},
              ":owner": meta["ownerUserId"], ":active": True, ":revision": str(uuid.uuid4()), ":now": _iso_now()}
    condition = ("ownerUserId = :owner AND (attribute_not_exists(isActive) OR isActive = :active)"
                 " AND attribute_not_exists(archivedAt) AND attribute_not_exists(deletedAt)"
                 " AND attribute_not_exists(deletionStartedAt)")
    if meta.get("formRevision"):
        condition += " AND formRevision = :previous"
        values[":previous"] = meta["formRevision"]
    else:
        condition += " AND attribute_not_exists(formRevision)"
        for index, attribute in enumerate(("quizOrder", "quizQuestions", "participantFields")):
            if attribute in meta:
                placeholder = f":legacy{index}"
                condition += f" AND {attribute} = {placeholder}"
                values[placeholder] = meta[attribute]
            else:
                condition += f" AND attribute_not_exists({attribute})"
    if structural:
        condition += " AND attribute_not_exists(quizLockedAt)"
    try:
        updated = dynamo.Table(os.environ["GAMES_TABLE"]).update_item(
            Key={"gameId": meta["gameId"]},
            UpdateExpression="SET #form = :form, participantFields = :fields, quizOrder = :order, quizQuestions = :questions, formRevision = :revision, updatedAt = :now",
            ConditionExpression=condition, ExpressionAttributeNames=names, ExpressionAttributeValues=values,
            ReturnValues="ALL_NEW",
        )["Attributes"]
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise FormError("FormChanged") from None
        raise
    return operator_shape(dynamo, updated)


def participant_state(meta, player):
    return {"moment": moment(meta), "completed": completed(meta, player),
            "finished": finished(meta, player), "questions": questions(meta),
            "revision": revision(meta)}


def submit(dynamo, meta, player, answers, expected_revision):
    if moment(meta) == "disabled":
        raise FormError("QuizDisabled")
    if expected_revision != revision(meta):
        raise FormError("FormChanged")
    if moment(meta) == "after_game" and not finished(meta, player):
        raise FormError("GameNotFinished")
    qs = questions(meta)
    if (not isinstance(answers, dict) or set(answers) != {q["id"] for q in qs} or not qs
            or any(answers[q["id"]] not in [o["payload"] for o in q["options"]] for q in qs)):
        raise FormError("InvalidQuizAnswers", 400)
    previous = get_game_type_blob(player, meta["gameType"])
    if completed(meta, player):
        if previous.get("quizAnswers") != answers:
            raise FormError("QuizAlreadyCompleted")
        return
    types = player.get("type") or {}
    now = _iso_now()
    next_types = {**types, meta["gameType"]: {**previous, "quizAnswers": answers,
                                           "quizCompleted": True, "quizCompletedAt": now}}
    if moment(meta) == "raffle":
        next_types[meta["gameType"]]["raffleEligible"] = True
    values = {":now": now, ":owner": meta["ownerUserId"]}
    condition = ("ownerUserId = :owner AND attribute_not_exists(archivedAt)"
                 " AND attribute_not_exists(deletedAt) AND attribute_not_exists(deletionStartedAt)")
    if meta.get("formRevision"):
        condition += " AND formRevision = :revision"
        values[":revision"] = meta["formRevision"]
    else:
        condition += " AND attribute_not_exists(formRevision)"
    try:
        dynamo.meta.client.transact_write_items(TransactItems=[
            {"Update": {"TableName": os.environ["GAMES_TABLE"], "Key": {"gameId": meta["gameId"]},
                        "UpdateExpression": "SET quizLockedAt = if_not_exists(quizLockedAt, :now)",
                        "ConditionExpression": condition, "ExpressionAttributeValues": values}},
            {"Update": {"TableName": os.environ["GAMEPLAYER_TABLE"],
                        "Key": {"gameId": meta["gameId"], "playerId": player["playerId"]},
                        "UpdateExpression": "SET #t = :next",
                        "ConditionExpression": "attribute_exists(playerId) AND #t = :previous",
                        "ExpressionAttributeNames": {"#t": "type"},
                        "ExpressionAttributeValues": {":next": next_types, ":previous": types}}},
        ])
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "TransactionCanceledException":
            current = dynamo.Table(os.environ["GAMEPLAYER_TABLE"]).get_item(
                Key={"gameId": meta["gameId"], "playerId": player["playerId"]}, ConsistentRead=True,
            ).get("Item") or {}
            if completed(meta, current) and get_game_type_blob(current, meta["gameType"]).get("quizAnswers") == answers:
                return
            raise FormError("FormChanged") from None
        raise
