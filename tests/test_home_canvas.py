from unittest.mock import MagicMock, patch
from decimal import Decimal
import json

import pytest

from stand_common import home_canvas
from tests.email_fake import FakeDynamoError
from tests.test_game_players import event, game, response_body


def test_new_and_legacy_canvas_shapes_are_distinct_and_deterministic():
    assert home_canvas.empty() == {
        "version": 1,
        "landscape": {"background": None, "items": []},
        "portrait": {"background": None, "items": []},
    }
    game = {"gameType": "T1MER", "showRaffleOnHome": False}
    first = home_canvas.initial(game)
    assert first == home_canvas.initial(game)
    assert {item["type"] for item in first["landscape"]["items"]} == {
        "logo", "players", "qr", "prizes", "ranking",
    }
    assert {item["type"] for item in first["portrait"]["items"]} == {
        "logo", "players", "qr", "prizes", "ranking",
    }
    assert home_canvas.resolved({"gameType": "INFOCARDS"}) != home_canvas.empty()


def _text(x=0, y=0, **extra):
    return {"id": "copy", "type": "text", "x": x, "y": y, "w": 2, "h": 1, **extra}


def test_saved_canvas_resolves_dynamodb_numbers_without_changing_revision():
    canvas = home_canvas.initial({"gameType": "INFOCARDS", "showRaffleOnHome": False})
    canvas["landscape"]["items"].append(_text(0, 9, text="Welcome", fontSize=32))
    stored = json.loads(json.dumps(canvas), parse_int=Decimal)
    assert type(stored["version"]) is Decimal
    result = home_canvas.resolved({"homeCanvas": stored})
    assert result == canvas
    assert type(result["version"]) is int
    for orientation in ("landscape", "portrait"):
        for item in result[orientation]["items"]:
            assert all(type(item[key]) is int for key in ("x", "y", "w", "h"))
            if "fontSize" in item:
                assert type(item["fontSize"]) is int
    assert home_canvas.revision(result) == home_canvas.revision(canvas)
    assert type(stored["version"]) is Decimal


@pytest.mark.parametrize(("field", "value", "error"), [
    ("version", Decimal("1.5"), "InvalidHomeCanvasVersion"),
    ("x", Decimal("0.5"), "InvalidHomeCanvasBounds"),
    ("w", Decimal("2.5"), "InvalidHomeCanvasBounds"),
    ("fontSize", Decimal("32.5"), "InvalidHomeCanvasFontSize"),
    ("x", True, "InvalidHomeCanvasBounds"),
])
def test_saved_canvas_normalization_does_not_accept_fractional_or_boolean_numbers(field, value, error):
    canvas = home_canvas.empty()
    canvas["landscape"]["items"] = [_text(fontSize=32)]
    if field == "version":
        canvas[field] = value
    else:
        canvas["landscape"]["items"][0][field] = value
    with pytest.raises(home_canvas.HomeCanvasError, match=error):
        home_canvas.resolved({"homeCanvas": canvas})


@pytest.mark.parametrize("item_type", sorted(home_canvas.ITEM_TYPES))
@pytest.mark.parametrize("background_color", ["transparent", "#12Ab34"])
def test_component_backgrounds_for_every_widget(item_type, background_color):
    canvas = home_canvas.empty()
    w, h = home_canvas.MIN_SIZE[item_type]
    canvas["landscape"]["items"] = [{
        "id": "widget", "type": item_type, "x": 0, "y": 0, "w": w, "h": h,
        "backgroundColor": background_color,
    }]
    assert home_canvas.validate(canvas) == canvas


@pytest.mark.parametrize("color", [None, 42, "red", "#123", "", "url(example)"])
def test_invalid_component_backgrounds_are_rejected(color):
    canvas = home_canvas.empty()
    canvas["landscape"]["items"] = [_text(backgroundColor=color)]
    with pytest.raises(home_canvas.HomeCanvasError, match="InvalidHomeCanvasBackgroundColor"):
        home_canvas.validate(canvas)


@pytest.mark.parametrize("mutate", [
    lambda canvas: canvas["landscape"]["items"].append(
        {"id": "bad", "type": "qr", "x": 0, "y": 0, "w": 4, "h": 5}
    ) or canvas["landscape"]["items"].append(
        {"id": "overlap", "type": "logo", "x": 0, "y": 0, "w": 3, "h": 2}
    ),
    lambda canvas: canvas["landscape"]["items"].append(
        {"id": "bad", "type": "text", "x": 31, "y": 0, "w": 2, "h": 1}
    ),
    lambda canvas: canvas["landscape"]["items"].append(
        {"id": "bad", "type": "logo", "x": 0, "y": 0, "w": 2, "h": 2}
    ),
    lambda canvas: canvas["landscape"]["items"].extend([
        {"id": "same", "type": "text", "x": 0, "y": 0, "w": 2, "h": 1},
        {"id": "same", "type": "text", "x": 2, "y": 0, "w": 2, "h": 1},
    ]),
    lambda canvas: canvas["landscape"]["items"].append(
        {"id": "bad", "type": "text", "x": 0, "y": 0, "w": 2, "h": 1, "color": "red"}
    ),
])
def test_rejects_out_of_bounds_overlap_bad_size_and_invalid_text(mutate):
    canvas = home_canvas.empty()
    mutate(canvas)
    with pytest.raises(home_canvas.HomeCanvasError):
        home_canvas.validate(canvas)


def test_text_limits_and_allowed_fields():
    canvas = home_canvas.empty()
    canvas["landscape"]["items"] = [
        _text(1, 2, text="Welcome", fontSize=32, color="#abcdef", align="center"),
    ]
    assert home_canvas.validate(canvas) == canvas
    for fields in (
        {"text": "x" * 1001},
        {"fontSize": 15},
        {"fontSize": True},
        {"align": "justify"},
        {"custom": "not allowed"},
    ):
        invalid = home_canvas.empty()
        invalid["landscape"]["items"] = [_text(1, 2, **fields)]
        with pytest.raises(home_canvas.HomeCanvasError):
            home_canvas.validate(invalid)


@pytest.mark.parametrize("item_type", sorted(home_canvas.ITEM_TYPES))
def test_optional_canvas_and_component_styles_survive_normalization(item_type):
    canvas = home_canvas.empty()
    style = {"backgroundColor": "#123456", "accentColor": "#AbCdEf", "color": "#ffffff", "fontSize": 96, "bold": True}
    canvas["landscape"].update(style)
    w, h = home_canvas.MIN_SIZE[item_type]
    canvas["landscape"]["items"] = [{
        "id": "styled", "type": item_type, "x": 0, "y": 0, "w": w, "h": h, **style,
    }]
    stored = json.loads(json.dumps(canvas), parse_int=Decimal)
    assert home_canvas.resolved({"homeCanvas": stored}) == canvas
    assert home_canvas.validate(canvas)["portrait"] == {"background": None, "items": []}


@pytest.mark.parametrize(("field", "value"), [
    ("backgroundColor", "red"), ("accentColor", "#fff"), ("color", None),
    ("fontSize", 15), ("fontSize", 161), ("fontSize", 24.5),
    ("fontSize", True), ("fontSize", "48"), ("accentColor", 123),
    ("bold", 1), ("bold", "true"), ("bold", None),
])
@pytest.mark.parametrize("component", [False, True])
def test_invalid_style_fields_rejected_at_both_levels(field, value, component):
    canvas = home_canvas.empty()
    if component:
        canvas["landscape"]["items"] = [_text(**{field: value})]
    else:
        canvas["landscape"][field] = value
    with pytest.raises(home_canvas.HomeCanvasError):
        home_canvas.validate(canvas)


def test_canvas_background_must_be_solid_hex_and_unknown_style_fields_are_rejected():
    for fields in ({"backgroundColor": "transparent"}, {"fontFamily": "Arial"}):
        canvas = home_canvas.empty()
        canvas["landscape"].update(fields)
        with pytest.raises(home_canvas.HomeCanvasError):
            home_canvas.validate(canvas)


def test_canvas_validation_rejects_duplicate_singletons_and_more_than_forty_items():
    canvas = home_canvas.empty()
    canvas["landscape"]["items"] = [
        {"id": "qr1", "type": "qr", "x": 0, "y": 0, "w": 4, "h": 5},
        {"id": "qr2", "type": "qr", "x": 4, "y": 0, "w": 4, "h": 5},
    ]
    with pytest.raises(home_canvas.HomeCanvasError, match="DuplicateHomeCanvasItem"):
        home_canvas.validate(canvas)
    canvas["landscape"]["items"] = [
        _text((index % 16) * 2, (index // 16) * 2, id=f"text-{index}")
        for index in range(41)
    ]
    with pytest.raises(home_canvas.HomeCanvasError, match="InvalidHomeCanvasItems"):
        home_canvas.validate(canvas)


@pytest.mark.parametrize(("game_type", "item_type", "raffle_enabled"), [
    ("INFOCARDS", "ranking", True),
    ("T1MER", "raffle", False),
])
def test_save_availability_respects_game_ranking_and_raffle_flags(game_type, item_type, raffle_enabled):
    canvas = home_canvas.empty()
    canvas["landscape"]["items"] = [{
        "id": item_type, "type": item_type, "x": 0, "y": 0, "w": 4, "h": 5,
    }]
    with pytest.raises(home_canvas.HomeCanvasError):
        home_canvas.validate_availability(
            canvas, {"gameType": game_type, "showRaffleOnHome": raffle_enabled},
        )


def _canvas_event(method, body=None, query=None, owner="owner"):
    return event(method, "/v1/game", body=body, query=query, owner=owner)


def test_home_read_returns_minimal_seeded_canvas_even_when_read_only():
    table = MagicMock()
    table.get_item.return_value = {"Item": {
        "gameId": "ABC123", "gameType": "T1MER", "ownerUserId": "owner",
        "isActive": False, "showRaffleOnHome": False,
    }}
    with patch.object(game, "games_table", table):
        response = game.lambda_handler(
            _canvas_event("GET", query={"gameId": "ABC123", "home": "true"}), None,
        )
    payload = response_body(response)
    assert response["statusCode"] == 200, payload
    assert set(payload) == {
        "ok", "gameId", "gameType", "homeCanvas", "homeRevision",
        "homeBackgroundUrls", "readOnly", "showRaffleOnHome",
    }
    assert payload["readOnly"] is True
    assert payload["showRaffleOnHome"] is False
    assert "ranking" in {item["type"] for item in payload["homeCanvas"]["landscape"]["items"]}
    assert "raffle" not in {item["type"] for item in payload["homeCanvas"]["landscape"]["items"]}
    assert payload["homeBackgroundUrls"] == {"landscape": None, "portrait": None}


def test_existing_game_get_adds_fresh_signed_home_background_urls():
    canvas = home_canvas.empty()
    canvas["landscape"]["background"] = {
        "key": "users/owner/home-backgrounds/scene.png", "type": "image",
    }
    table, s3 = MagicMock(), MagicMock()
    table.get_item.return_value = {"Item": {
        "gameId": "ABC123", "gameType": "INFOCARDS", "ownerUserId": "owner",
        "isActive": True, "homeCanvas": canvas, "homeRevision": "revision",
    }}
    s3.generate_presigned_url.return_value = "https://fresh-signed-url"
    with patch.object(game, "games_table", table), patch.object(game, "prize_s3", s3), \
            patch.object(game, "USER_MEDIA_BUCKET", "media"):
        response = game.lambda_handler(
            _canvas_event("GET", query={"gameId": "ABC123"}), None,
        )
    payload = response_body(response)
    assert response["statusCode"] == 200
    assert payload["homeCanvas"] == canvas
    assert payload["homeBackgroundUrls"] == {
        "landscape": "https://fresh-signed-url", "portrait": None,
    }
    assert s3.generate_presigned_url.call_args.kwargs["Params"]["Key"] == canvas["landscape"]["background"]["key"]


def test_home_read_and_background_save_enforce_owner_and_key_prefix():
    item = {
        "gameId": "ABC123", "gameType": "INFOCARDS", "ownerUserId": "owner",
        "isActive": True, "homeCanvas": home_canvas.empty(), "homeRevision": "r1",
    }
    table = MagicMock()
    table.get_item.return_value = {"Item": item}
    with patch.object(game, "games_table", table):
        forbidden = game.lambda_handler(
            _canvas_event("GET", query={"gameId": "ABC123", "home": "true"}, owner="other"), None,
        )
    assert forbidden["statusCode"] == 403
    canvas = home_canvas.empty()
    canvas["landscape"]["background"] = {
        "key": "users/other/home-backgrounds/not-owned.png", "type": "image",
    }
    with patch.object(game, "games_table", table), patch.object(game, "_is_user_pro", return_value=True):
        rejected = game.lambda_handler(
            _canvas_event("PUT", body={
                "gameId": "ABC123", "homeCanvas": canvas, "expectedHomeRevision": "r1",
            }), None,
        )
    assert rejected["statusCode"] == 400
    assert response_body(rejected)["error"] == "InvalidHomeCanvasBackground"
    table.update_item.assert_not_called()


def test_game_creation_persists_explicit_empty_home_canvas():
    table = MagicMock()
    with patch.object(game, "games_table", table), \
            patch.object(game, "_get_user", return_value={"plan": "PRO"}), \
            patch.object(game, "_is_user_pro", return_value=True), \
            patch.object(game, "_new_join_code", return_value="ABC123"), \
            patch.object(game, "_load_default_quiz_from_catalog", return_value=(None, None)):
        response = game.lambda_handler(
            _canvas_event("POST", body={"gameType": "INFOCARDS"}), None,
        )
    assert response["statusCode"] == 201
    created = table.put_item.call_args.kwargs["Item"]
    assert created["homeCanvas"] == home_canvas.empty()


def test_free_home_save_allows_layout_and_text_but_uses_independent_revision():
    item = {
        "gameId": "ABC123", "gameType": "INFOCARDS", "ownerUserId": "owner",
        "isActive": True, "showRaffleOnHome": False,
    }
    canvas = home_canvas.initial(item)
    canvas["landscape"]["items"].append(_text(0, 9, text="Welcome"))
    canvas["portrait"]["items"].append(_text(0, 20, text="Welcome"))
    canvas["landscape"]["items"][0]["backgroundColor"] = "#123456"
    canvas["landscape"]["fontSize"] = 96
    canvas["landscape"]["bold"] = True
    canvas["landscape"]["items"][0]["bold"] = False
    canvas["landscape"]["items"][0]["fontSize"] = 32
    canvas["portrait"]["items"][0]["backgroundColor"] = "transparent"
    expected = home_canvas.revision(home_canvas.initial(item))
    table = MagicMock()
    table.get_item.return_value = {"Item": item}
    with patch.object(game, "games_table", table), \
            patch.object(game, "_get_user", return_value={"plan": "FREE"}):
        response = game.lambda_handler(
            _canvas_event("PUT", body={
                "gameId": "ABC123", "homeCanvas": canvas, "expectedHomeRevision": expected,
            }), None,
        )
    payload = response_body(response)
    assert response["statusCode"] == 200
    assert payload["homeCanvas"] == canvas
    assert payload["homeRevision"] != expected
    assert payload["readOnly"] is False
    update = table.update_item.call_args.kwargs
    assert "homeRevision" in update["UpdateExpression"]
    assert "attribute_not_exists(#homeCanvas)" in update["ConditionExpression"]
    assert "formRevision" not in update["ConditionExpression"]
    assert "#gameType = :gameType" in update["ConditionExpression"]
    expressions = update["UpdateExpression"] + " " + update["ConditionExpression"]
    assert all(alias in expressions for alias in update["ExpressionAttributeNames"])


@pytest.mark.parametrize("change", ["background", "text-color"])
def test_free_home_save_rejects_premium_background_or_custom_text_color(change):
    item = {
        "gameId": "ABC123", "gameType": "INFOCARDS", "ownerUserId": "owner",
        "isActive": True, "showRaffleOnHome": False,
    }
    canvas = home_canvas.initial(item)
    if change == "background":
        canvas["landscape"]["background"] = {
            "key": "users/owner/home-backgrounds/image.png", "type": "image",
        }
    else:
        canvas["landscape"]["items"].append(_text(0, 9, text="Welcome", color="#123456"))
    table = MagicMock()
    table.get_item.return_value = {"Item": item}
    with patch.object(game, "games_table", table), \
            patch.object(game, "_get_user", return_value={"plan": "FREE"}):
        response = game.lambda_handler(
            _canvas_event("PUT", body={
                "gameId": "ABC123", "homeCanvas": canvas,
                "expectedHomeRevision": home_canvas.revision(home_canvas.initial(item)),
            }), None,
        )
    assert response["statusCode"] == 403
    assert response_body(response)["error"] == "PlanUpgradeRequired"
    table.update_item.assert_not_called()


@pytest.mark.parametrize("orientation", ["landscape", "portrait"])
@pytest.mark.parametrize(("field", "component"), [
    ("backgroundColor", False), ("accentColor", False), ("color", False),
    ("accentColor", True), ("color", True),
])
def test_free_home_style_color_changes_are_gated(orientation, field, component):
    canvas = home_canvas.empty()
    if component:
        canvas[orientation]["items"] = [_text(**{field: "#123456"})]
    else:
        canvas[orientation][field] = "#123456"
    item = {
        "gameId": "ABC123", "gameType": "INFOCARDS", "ownerUserId": "owner",
        "isActive": True, "homeCanvas": home_canvas.empty(), "homeRevision": "r1",
    }
    table = MagicMock()
    table.get_item.return_value = {"Item": item}
    with patch.object(game, "games_table", table), \
            patch.object(game, "_get_user", return_value={"plan": "FREE"}):
        response = game.lambda_handler(_canvas_event("PUT", body={
            "gameId": "ABC123", "homeCanvas": canvas, "expectedHomeRevision": "r1",
        }), None)
    assert response["statusCode"] == 403
    assert response_body(response)["error"] == "PlanUpgradeRequired"
    table.update_item.assert_not_called()


@pytest.mark.parametrize("pro", [False, True])
def test_style_save_preserves_premium_colors_and_independent_orientations(pro):
    stored = home_canvas.empty()
    stored["landscape"].update(backgroundColor="#123456", accentColor="#abcdef", color="#ffffff")
    stored["landscape"]["items"] = [_text(color="#fedcba", accentColor="#654321")]
    canvas = json.loads(json.dumps(stored))
    canvas["landscape"]["fontSize"] = 96
    canvas["landscape"]["items"][0]["fontSize"] = 32
    canvas["portrait"]["fontSize"] = 24
    if pro:
        canvas["portrait"].update(backgroundColor="#222222", accentColor="#aaaaaa", color="#eeeeee")
    item = {
        "gameId": "ABC123", "gameType": "INFOCARDS", "ownerUserId": "owner",
        "isActive": True, "homeCanvas": stored, "homeRevision": "r1",
    }
    table = MagicMock()
    table.get_item.return_value = {"Item": item}
    with patch.object(game, "games_table", table), \
            patch.object(game, "_is_user_pro", return_value=pro):
        response = game.lambda_handler(_canvas_event("PUT", body={
            "gameId": "ABC123", "homeCanvas": canvas, "expectedHomeRevision": "r1",
        }), None)
    assert response["statusCode"] == 200
    assert response_body(response)["homeCanvas"] == canvas
    assert table.update_item.call_args.kwargs["ExpressionAttributeValues"][":canvas"] == canvas


def test_home_save_rejects_stale_revision_and_inactive_game_without_writes():
    item = {
        "gameId": "ABC123", "gameType": "INFOCARDS", "ownerUserId": "owner",
        "isActive": True, "showRaffleOnHome": False, "homeCanvas": home_canvas.empty(),
        "homeRevision": "current",
    }
    table = MagicMock()
    table.get_item.return_value = {"Item": item}
    with patch.object(game, "games_table", table), patch.object(game, "_get_user", return_value={"plan": "PRO"}):
        stale = game.lambda_handler(
            _canvas_event("PUT", body={
                "gameId": "ABC123", "homeCanvas": home_canvas.empty(),
                "expectedHomeRevision": "old",
            }), None,
        )
    assert stale["statusCode"] == 409
    table.update_item.assert_not_called()
    table.get_item.return_value = {"Item": {**item, "isActive": False}}
    with patch.object(game, "games_table", table):
        inactive = game.lambda_handler(
            _canvas_event("PUT", body={
                "gameId": "ABC123", "homeCanvas": home_canvas.empty(),
                "expectedHomeRevision": "current",
            }), None,
        )
    assert inactive["statusCode"] == 409
    table.update_item.assert_not_called()


def test_home_save_reports_concurrent_revision_change():
    item = {
        "gameId": "ABC123", "gameType": "INFOCARDS", "ownerUserId": "owner",
        "isActive": True, "homeCanvas": home_canvas.empty(), "homeRevision": "current",
    }
    table = MagicMock()
    table.get_item.return_value = {"Item": item}
    table.update_item.side_effect = FakeDynamoError("ConditionalCheckFailedException")
    with patch.object(game, "games_table", table), patch.object(game, "_is_user_pro", return_value=True):
        response = game.lambda_handler(
            _canvas_event("PUT", body={
                "gameId": "ABC123", "homeCanvas": home_canvas.empty(),
                "expectedHomeRevision": "current",
            }), None,
        )
    assert response["statusCode"] == 409
    assert response_body(response)["error"] == "HomeCanvasChanged"


def test_pro_home_save_validates_owned_media_and_signs_background_on_response():
    item = {
        "gameId": "ABC123", "gameType": "INFOCARDS", "ownerUserId": "owner",
        "isActive": True, "homeCanvas": home_canvas.empty(), "homeRevision": "r1",
    }
    canvas = home_canvas.empty()
    canvas["landscape"]["background"] = {
        "key": "users/owner/home-backgrounds/wallpaper.png", "type": "image",
    }
    table, s3 = MagicMock(), MagicMock()
    table.get_item.return_value = {"Item": item}
    s3.head_object.return_value = {"ContentType": "image/png", "ContentLength": 10}
    s3.generate_presigned_url.return_value = "https://signed"
    with patch.object(game, "games_table", table), patch.object(game, "_is_user_pro", return_value=True), \
            patch.object(game, "_get_user", return_value={"plan": "PRO"}), \
            patch.object(game, "USER_MEDIA_BUCKET", "media"), patch.object(game, "prize_s3", s3):
        response = game.lambda_handler(
            _canvas_event("PUT", body={
                "gameId": "ABC123", "homeCanvas": canvas, "expectedHomeRevision": "r1",
            }), None,
        )
    payload = response_body(response)
    assert response["statusCode"] == 200, payload
    assert payload["homeBackgroundUrls"] == {"landscape": "https://signed", "portrait": None}
    assert s3.head_object.call_args.kwargs["Key"] == "users/owner/home-backgrounds/wallpaper.png"


def test_home_background_keys_cannot_be_signed_for_another_owner():
    canvas = home_canvas.empty()
    canvas["landscape"]["background"] = {
        "key": "users/another/home-backgrounds/file.png", "type": "image",
    }
    with pytest.raises(home_canvas.HomeCanvasError):
        home_canvas.resolved({"homeCanvas": canvas}, "owner")
