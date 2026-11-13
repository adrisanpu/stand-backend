import json
from unittest.mock import MagicMock

import pytest
from tests.test_stack_integration import load, ROOT, operator_event
from tests.email_fake import FakeDynamoError

cards_api = load("admin_infocards_get", ROOT / "src" / "stand_prod_infocards_fn_get" / "lambda_function.py")
process_api = load("admin_infocards_process", ROOT / "src" / "stand_prod_infocards_fn_process" / "lambda_function.py")
upload_api = load("admin_infocards_upload", ROOT / "src" / "stand_prod_infocards_fn_upload_url" / "lambda_function.py")
META = {"gameId": "ABC123", "gameType": "INFOCARDS", "ownerUserId": "owner", "isActive": True,
        "type": {"INFOCARDS": {"cardLayouts": {"one": "left"}, "quizCompletedMessage": "Done"}}}


@pytest.mark.parametrize("module,body", [
    (cards_api, {"cards": []}),
    (process_api, {"text": "Product"}),
    (upload_api, {"files": [{"fileName": "x.png", "contentType": "image/png"}]}),
])
def test_inactive_infocards_reject_all_config_writes(module, body, monkeypatch):
    table = MagicMock()
    table.get_item.return_value = {"Item": {**META, "isActive": False}}
    monkeypatch.setattr(module, "games_table", table)
    event = operator_event({"gameId": "ABC123", **body})
    event["httpMethod"] = "PUT" if module is cards_api else "POST"
    result = module.lambda_handler(event, None)
    assert result["statusCode"] == 409, result
    table.update_item.assert_not_called()

@pytest.mark.parametrize("layout", ["top", "left", "right", "bottom"])
def test_layouts_persist_with_cards_and_owner_active_write_guards(monkeypatch, layout):
    table = MagicMock()
    table.get_item.return_value = {"Item": META}
    monkeypatch.setattr(cards_api, "games_table", table)
    result = cards_api._handle_put(operator_event({"gameId": "ABC123", "cards": [
        {"id": "one", "title": "Title", "body": "Body"},
    ], "cardLayouts": {"one": layout}}), "owner")
    assert result["statusCode"] == 200
    for call in table.update_item.call_args_list:
        assert "ownerUserId = :owner AND isActive = :active" in call.kwargs["ConditionExpression"]
        assert call.kwargs["ExpressionAttributeValues"][":active"] is True
    payload = table.update_item.call_args.kwargs["ExpressionAttributeValues"][":payload"]
    assert payload["cardLayouts"] == {"one": layout}
    assert payload["quizCompletedMessage"] == "Done"
    assert len(payload["cards"]) == 1


@pytest.mark.parametrize("layouts", [[], None, {"one": "diagonal"}, {"one": ["left"]}])
def test_invalid_layouts_do_not_write(monkeypatch, layouts):
    table = MagicMock()
    table.get_item.return_value = {"Item": META}
    monkeypatch.setattr(cards_api, "games_table", table)
    result = cards_api._handle_put(operator_event({"gameId": "ABC123", "cards": [], "cardLayouts": layouts}), "owner")
    assert result["statusCode"] == 400
    table.update_item.assert_not_called()


def test_configuration_refuses_close_between_read_and_write(monkeypatch):
    table = MagicMock()
    table.get_item.return_value = {"Item": META}
    table.update_item.side_effect = FakeDynamoError("ConditionalCheckFailedException")
    monkeypatch.setattr(cards_api, "games_table", table)
    monkeypatch.setattr(cards_api, "ClientError", FakeDynamoError)
    result = cards_api._handle_put(operator_event({"gameId": "ABC123", "cards": []}), "owner")
    assert result["statusCode"] == 409


def test_regeneration_preserves_layouts_and_completion_configuration(monkeypatch):
    table = MagicMock()
    table.get_item.return_value = {"Item": META}
    monkeypatch.setattr(process_api, "games_table", table)
    monkeypatch.setattr(process_api, "_call_bedrock", lambda _: json.dumps({"cards": [{"title": "Title", "body": "Body"}]}))
    result = process_api.lambda_handler(operator_event({"gameId": "ABC123", "text": "Product"}), None)
    assert result["statusCode"] == 200, result
    assert "quizGenerated" not in json.loads(result["body"])
    payload = table.update_item.call_args.kwargs["ExpressionAttributeValues"][":payload"]
    assert payload["cardLayouts"] == META["type"]["INFOCARDS"]["cardLayouts"]
    assert payload["quizCompletedMessage"] == "Done"
