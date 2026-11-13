import copy
from unittest.mock import MagicMock

import pytest
from stand_common import game_forms as forms


@pytest.fixture
def meta():
    return {
        "gameId": "ABC123", "gameType": "T1MER", "ownerUserId": "owner", "isActive": True,
        "formConfig": {**forms.DEFAULT, "quizMoment": "after_registration"},
        "formRevision": "revision-1", "quizOrder": ["q1"],
        "quizQuestions": {"q1": {"text": "Opinion?", "options": [
            {"title": "Good", "payload": "ABC123_q1_a"}, {"title": "Great", "payload": "ABC123_q1_b"},
        ]}},
        "participantFields": {"email": {"required": True}, "phone": {"required": True, "enabled": True}},
    }


@pytest.fixture
def player():
    return {"gameId": "ABC123", "playerId": 1, "type": {"T1MER": {"score": 123}}}


@pytest.fixture
def db(monkeypatch):
    monkeypatch.setenv("GAMES_TABLE", "games")
    monkeypatch.setenv("GAMEPLAYER_TABLE", "players")
    db = MagicMock()
    db.Table.return_value.query.return_value = {"Items": []}
    return db


def draft(meta):
    return {"formConfig": forms.config(meta), "questions": forms.questions(meta),
            "participantFields": meta["participantFields"], "expectedRevision": forms.revision(meta)}


@pytest.mark.parametrize("at", ["after_registration", "after_game", "prize", "raffle"])
def test_only_selected_moment_is_required(meta, player, at):
    meta["formConfig"]["quizMoment"] = at
    for candidate in forms.MOMENTS - {"disabled"}:
        assert forms.pending(meta, player, candidate) == (at == candidate)
    player["type"]["T1MER"]["quizCompleted"] = True
    assert not forms.pending(meta, player, at)


def test_legacy_config_does_not_activate_new_gates(meta, player):
    meta.pop("formConfig")
    assert forms.moment(meta) == "disabled"
    forms.require(meta, player, "prize")


@pytest.mark.parametrize("changes", [
    {"quizMoment": []}, {"quizMoment": "unknown"}, {"registrationTitle": 12},
    {"logoKey": "users/another/logos/file.png"}, {"bannerKey": "https://example.test/x"},
    {"appearance": {"colors": {"primary": "red", "accent": "#ffffff"}, "themeMode": "light"}},
])
def test_rejects_invalid_presentation(changes):
    with pytest.raises(forms.FormError):
        forms.validate_config({**forms.DEFAULT, **changes}, "owner")


def test_question_limits(meta):
    with pytest.raises(forms.FormError):
        forms.normalize_questions(forms.questions(meta) * 6, "ABC123")
    q = forms.questions(meta)[0]
    q["options"] *= 2
    with pytest.raises(forms.FormError):
        forms.normalize_questions([q], "ABC123")


def test_locked_checks_all_query_pages(db, meta, player):
    player["type"]["T1MER"]["quizCompleted"] = True
    db.Table.return_value.query.side_effect = [
        {"Items": [], "LastEvaluatedKey": {"gameId": "ABC123", "playerId": 1}},
        {"Items": [player]},
    ]
    assert forms.locked(db, meta)
    assert db.Table.return_value.query.call_args.kwargs["ExclusiveStartKey"]["playerId"] == 1


def test_edit_locked_quiz_rejected_but_presentation_allowed(db, meta):
    meta["quizLockedAt"] = "now"
    request = draft(meta)
    request["formConfig"]["quizMoment"] = "prize"
    with pytest.raises(forms.FormError, match="QuizLocked"):
        forms.save(db, meta, request)
    db.Table.return_value.update_item.assert_not_called()
    request = draft(meta)
    request["formConfig"]["registrationTitle"] = "Welcome"
    updated = {**meta, "formConfig": request["formConfig"]}
    db.Table.return_value.update_item.return_value = {"Attributes": updated}
    assert forms.save(db, meta, request)["formConfig"]["registrationTitle"] == "Welcome"
    assert "attribute_not_exists(quizLockedAt)" not in db.Table.return_value.update_item.call_args.kwargs["ConditionExpression"]


def test_structure_edit_has_atomic_first_response_guard(db, meta):
    request = draft(meta)
    request["questions"][0]["text"] = "Different?"
    db.Table.return_value.update_item.return_value = {"Attributes": meta}
    forms.save(db, meta, request)
    condition = db.Table.return_value.update_item.call_args.kwargs["ConditionExpression"]
    assert "attribute_not_exists(quizLockedAt)" in condition
    assert "formRevision = :previous" in condition


@pytest.mark.parametrize("at", ["after_registration", "after_game", "prize", "raffle"])
def test_locked_quiz_can_be_disabled_and_restored_without_changing_questions(db, meta, at):
    meta["quizLockedAt"] = "now"
    meta["formConfig"]["quizMoment"] = at
    request = draft(meta)
    request["formConfig"]["quizMoment"] = "disabled"
    updated = {**meta, "formConfig": request["formConfig"], "quizPreviousMoment": at}
    db.Table.return_value.update_item.return_value = {"Attributes": updated}
    result = forms.save(db, meta, request)
    call = db.Table.return_value.update_item.call_args.kwargs
    assert result["quizPreviousMoment"] == at
    assert result["questions"] == forms.questions(meta)
    assert call["ExpressionAttributeValues"][":quizPreviousMoment"] == at
    assert "attribute_not_exists(quizLockedAt)" not in call["ConditionExpression"]
    assert "formRevision = :previous" in call["ConditionExpression"]

    restore = draft(updated)
    restore["formConfig"]["quizMoment"] = at
    db.Table.return_value.update_item.return_value = {"Attributes": {**updated, "formConfig": restore["formConfig"]}}
    assert forms.save(db, updated, restore)["formConfig"]["quizMoment"] == at
    assert "attribute_not_exists(quizLockedAt)" not in db.Table.return_value.update_item.call_args.kwargs["ConditionExpression"]


def test_disable_cannot_modify_locked_questions_or_restore_a_different_moment(db, meta):
    meta["quizLockedAt"] = "now"
    request = draft(meta)
    request["formConfig"]["quizMoment"] = "disabled"
    request["questions"][0]["text"] = "Changed?"
    with pytest.raises(forms.FormError, match="QuizLocked"):
        forms.save(db, meta, request)

    meta["formConfig"]["quizMoment"] = "disabled"
    meta["quizPreviousMoment"] = "after_registration"
    request = draft(meta)
    request["formConfig"]["quizMoment"] = "prize"
    with pytest.raises(forms.FormError, match="QuizLocked"):
        forms.save(db, meta, request)
    db.Table.return_value.update_item.assert_not_called()


def test_stale_config_rejected_without_writing(db, meta):
    request = draft(meta)
    request["expectedRevision"] = "stale"
    with pytest.raises(forms.FormError, match="FormChanged"):
        forms.save(db, meta, request)
    db.Table.return_value.update_item.assert_not_called()


def test_submit_atomically_locks_questions_and_preserves_game_state(db, meta, player):
    snapshot = copy.deepcopy(player)
    forms.submit(db, meta, player, {"q1": "ABC123_q1_b"}, forms.revision(meta))
    operations = db.meta.client.transact_write_items.call_args.kwargs["TransactItems"]
    assert len(operations) == 2
    assert "quizLockedAt" in operations[0]["Update"]["UpdateExpression"]
    update = operations[1]["Update"]
    next_type = update["ExpressionAttributeValues"][":next"]["T1MER"]
    assert next_type["score"] == 123
    assert next_type["quizCompleted"]
    assert next_type["quizCompletedAt"]
    assert "attribute_exists(playerId)" in update["ConditionExpression"]
    assert player == snapshot


@pytest.mark.parametrize("answers", [{}, {"q1": "invalid"}, {"q1": "ABC123_q1_a", "q2": "extra"}, None])
def test_invalid_answers_do_not_write(db, meta, player, answers):
    with pytest.raises(forms.FormError, match="InvalidQuizAnswers"):
        forms.submit(db, meta, player, answers, forms.revision(meta))
    db.meta.client.transact_write_items.assert_not_called()


def test_submission_is_idempotent_and_cannot_change_answers(db, meta, player):
    answers = {"q1": "ABC123_q1_a"}
    player["type"]["T1MER"].update(quizCompleted=True, quizAnswers=answers)
    forms.submit(db, meta, player, answers, forms.revision(meta))
    db.meta.client.transact_write_items.assert_not_called()
    with pytest.raises(forms.FormError, match="QuizAlreadyCompleted"):
        forms.submit(db, meta, player, {"q1": "ABC123_q1_b"}, forms.revision(meta))


def test_raffle_submission_enables_automatic_entry(db, meta, player):
    meta["formConfig"]["quizMoment"] = "raffle"
    forms.submit(db, meta, player, {"q1": "ABC123_q1_a"}, forms.revision(meta))
    operation = db.meta.client.transact_write_items.call_args.kwargs["TransactItems"][1]["Update"]
    assert operation["ExpressionAttributeValues"][":next"]["T1MER"]["raffleEligible"]


def test_after_game_cannot_be_submitted_before_real_completion(db, meta, player):
    meta["formConfig"]["quizMoment"] = "after_game"
    player["type"]["T1MER"].pop("score")
    with pytest.raises(forms.FormError, match="GameNotFinished"):
        forms.submit(db, meta, player, {"q1": "ABC123_q1_a"}, forms.revision(meta))


@pytest.mark.parametrize("game_type,blob,expected", [
    ("STACK", {"attempt": {"status": "started"}}, False),
    ("STACK", {"attempt": {"status": "finished"}}, True),
    ("RULET4", {"result": {"index": 1}}, True),
    ("T1MER", {"score": 0}, True),
])
def test_finished_uses_authoritative_result(game_type, blob, expected):
    assert forms.finished({"gameType": game_type}, {"type": {game_type: blob}}) is expected


def test_concurrent_submission_reloads_identical_answer_as_success(db, meta, player, monkeypatch):
    from tests.email_fake import FakeDynamoError
    monkeypatch.setattr(forms, "ClientError", FakeDynamoError)
    answers = {"q1": "ABC123_q1_a"}
    saved = copy.deepcopy(player)
    saved["type"]["T1MER"].update(quizCompleted=True, quizAnswers=answers)
    db.meta.client.transact_write_items.side_effect = FakeDynamoError("TransactionCanceledException")
    db.Table.return_value.get_item.return_value = {"Item": saved}
    forms.submit(db, meta, player, answers, forms.revision(meta))


def test_concurrent_question_change_is_reported_as_conflict(db, meta, player, monkeypatch):
    from tests.email_fake import FakeDynamoError
    monkeypatch.setattr(forms, "ClientError", FakeDynamoError)
    db.meta.client.transact_write_items.side_effect = FakeDynamoError("TransactionCanceledException")
    db.Table.return_value.get_item.return_value = {"Item": player}
    with pytest.raises(forms.FormError, match="FormChanged"):
        forms.submit(db, meta, player, {"q1": "ABC123_q1_a"}, forms.revision(meta))


def test_partial_historical_answers_also_lock_structure(db, meta, player):
    player["type"]["T1MER"]["quizAnswers"] = {"q1": "ABC123_q1_a"}
    db.Table.return_value.query.return_value = {"Items": [player]}
    assert forms.locked(db, meta)


def test_pending_prize_does_not_expose_a_usable_token(meta, player, monkeypatch):
    import json
    from tests.test_public_web import public_mod
    meta["gameType"] = "EMPAREJA2"
    meta["formConfig"]["quizMoment"] = "prize"
    monkeypatch.setattr(public_mod.kivly_registration, "require_player", lambda *_: None)
    monkeypatch.setattr(public_mod, "_empareja2_assignment_from_player", lambda *_: {"playerId": 1})
    monkeypatch.setattr(public_mod, "_empareja2_prizes", lambda *_: [{"kind": "validation", "token": "secret", "delivered": False}])
    response = public_mod._player_response(meta, player, "ABC123", 1, "session")
    value = json.loads(response["body"])
    assert value["prizes"][0]["token"] is None
    assert value["quiz"]["prizePending"] is True


def test_operator_form_route_persists_and_returns_confirmed_contract(db, meta, monkeypatch):
    import json
    from tests.test_participant_fields import game, operator_event
    request = draft(meta)
    request["formConfig"]["registrationTitle"] = "Welcome"
    updated = {**meta, "formConfig": request["formConfig"], "formRevision": "revision-2"}
    table = db.Table.return_value
    table.get_item.return_value = {"Item": meta}
    table.update_item.return_value = {"Attributes": updated}
    monkeypatch.setattr(game, "dynamodb", db)
    monkeypatch.setattr(game, "games_table", table)
    monkeypatch.setattr(game, "_is_user_pro", lambda _: True)
    result = game._handle_put(operator_event({"gameId": "ABC123", "form": request}))
    assert result["statusCode"] == 200
    value = json.loads(result["body"])
    assert value["formConfig"]["registrationTitle"] == "Welcome"
    assert value["joinUrl"] == "/play/ABC123"
    assert value["revision"] == forms.revision(updated)
    assert not value["locked"]


def test_operator_form_route_rejects_other_owner(db, meta, monkeypatch):
    from tests.test_participant_fields import game, operator_event
    table = db.Table.return_value
    table.get_item.return_value = {"Item": {**meta, "ownerUserId": "different"}}
    monkeypatch.setattr(game, "games_table", table)
    result = game._handle_put(operator_event({"gameId": "ABC123", "form": draft(meta)}))
    assert result["statusCode"] == 403
    table.update_item.assert_not_called()


def test_operator_game_metadata_includes_shared_event_appearance(db, meta, monkeypatch):
    import json
    from tests.test_participant_fields import game, operator_event
    meta["formConfig"].update({
        "logoKey": "users/owner/logos/logo.png",
        "bannerKey": "users/owner/logos/banner.png",
        "appearance": {"colors": {"primary": "#123456", "accent": "#654321"}, "themeMode": "light"},
    })
    table = db.Table.return_value
    table.get_item.return_value = {"Item": meta}
    monkeypatch.setattr(game, "games_table", table)
    monkeypatch.setattr(forms, "image_url", lambda key: f"https://signed.test/{key}" if key else None)
    event = operator_event({})
    event["queryStringParameters"] = {"gameId": "ABC123"}
    result = game._handle_get(event)
    assert result["statusCode"] == 200
    value = json.loads(result["body"])
    assert value["formConfig"]["appearance"] == meta["formConfig"]["appearance"]
    assert value["formLogoUrl"] == "https://signed.test/users/owner/logos/logo.png"
    assert value["formBannerUrl"] == "https://signed.test/users/owner/logos/banner.png"


def test_operator_form_route_rejects_quiz_change_without_plan(db, meta, monkeypatch):
    from tests.test_participant_fields import game, operator_event
    table = db.Table.return_value
    table.get_item.return_value = {"Item": meta}
    monkeypatch.setattr(game, "games_table", table)
    monkeypatch.setattr(game, "_is_user_pro", lambda _: False)
    request = draft(meta)
    request["formConfig"]["quizMoment"] = "prize"
    result = game._handle_put(operator_event({"gameId": "ABC123", "form": request}))
    assert result["statusCode"] == 403
    table.update_item.assert_not_called()


def test_operator_without_plan_can_disable_questionnaire(db, meta, monkeypatch):
    from tests.test_participant_fields import game, operator_event
    meta["quizLockedAt"] = "now"
    table = db.Table.return_value
    table.get_item.return_value = {"Item": meta}
    request = draft(meta)
    request["formConfig"]["quizMoment"] = "disabled"
    table.update_item.return_value = {"Attributes": {
        **meta, "formConfig": request["formConfig"], "quizPreviousMoment": "after_registration",
    }}
    monkeypatch.setattr(game, "dynamodb", db)
    monkeypatch.setattr(game, "games_table", table)
    monkeypatch.setattr(game, "_is_user_pro", lambda _: False)
    result = game._handle_put(operator_event({"gameId": "ABC123", "form": request}))
    assert result["statusCode"] == 200
    assert table.update_item.call_args.kwargs["ExpressionAttributeValues"][":quizPreviousMoment"] == "after_registration"
    disabled = table.update_item.return_value["Attributes"]
    table.get_item.return_value = {"Item": disabled}
    request = draft(disabled)
    request["formConfig"]["quizMoment"] = "after_registration"
    assert game._handle_put(operator_event({"gameId": "ABC123", "form": request}))["statusCode"] == 403
