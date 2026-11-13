import pytest

from stand_common.email_verification import RegistrationError, registration_values
from stand_common.utils import OPTIONAL_PARTICIPANT_FIELDS, normalize_participant_fields, web_participant_fields
from tests.test_email_verification import (
    rig, request_code, response_body, sent_code, join, resume_request, seed_returning_player,
)


@pytest.mark.parametrize("game_type", ["T1MER", "STACK", "EMPAREJA2", "RULET4", "SEMAFORO", "L3TRAS", "INFOCARDS", "IMPOSTOR"])
def test_phone_is_required_even_with_legacy_disabled_config(game_type):
    meta = {"gameType": game_type, "participantFields": {"phone": {"enabled": False, "required": False}}}
    assert web_participant_fields(game_type, meta["participantFields"])["phone"] == {"enabled": True, "required": True}
    with pytest.raises(RegistrationError, match="missing_phone"):
        registration_values(meta, {"email": "a@example.com", "username": "Ana"})


def enable_profile(rig):
    game = rig["dynamo"].rows("games")[0]
    game["participantFields"].update({field: {"enabled": True} for field in OPTIONAL_PARTICIPANT_FIELDS})
    rig["dynamo"].seed("games", game)


def test_optional_profile_is_bound_verified_and_persisted_without_public_exposure(rig):
    enable_profile(rig)
    profile = {"address": " Calle Mayor 1 ", "company": " ACME ", "sex": "female", "jobTitle": " Engineer ", "birthDate": "1990-02-20"}
    challenge = response_body(request_code(**profile))
    result = join(rig, challenge["challengeId"], sent_code(rig))
    assert result["statusCode"] == 200
    player = rig["dynamo"].rows("players")[0]
    for field, value in profile.items():
        assert player[field] == value.strip()
        assert field not in response_body(result)["assignment"]


def test_enabled_optional_fields_can_all_be_left_empty(rig):
    enable_profile(rig)
    challenge = response_body(request_code())
    assert join(rig, challenge["challengeId"], sent_code(rig))["statusCode"] == 200
    assert all(field not in rig["dynamo"].rows("players")[0] for field in OPTIONAL_PARTICIPANT_FIELDS)


@pytest.mark.parametrize("field,value", [
    ("address", "x" * 301), ("company", []), ("jobTitle", "A\x01B"),
    ("sex", "unknown"), ("sex", "Male"), ("sex", []), ("sex", 1),
    ("birthDate", "2025-02-30"), ("birthDate", "2999-01-01"), ("birthDate", "2000-2-1"),
])
def test_profile_validation_before_email_delivery(rig, field, value):
    enable_profile(rig)
    result = request_code(**{field: value})
    assert result["statusCode"] == 400
    assert response_body(result)["error"] == f"invalid_{field}"
    assert not rig["ses"].sent


def test_disabled_optional_values_are_not_collected(rig):
    challenge = response_body(request_code(company=["untrusted"], birthDate="bad", sex="invalid"))
    assert join(rig, challenge["challengeId"], sent_code(rig))["statusCode"] == 200
    assert "company" not in rig["dynamo"].rows("players")[0]
    assert "birthDate" not in rig["dynamo"].rows("players")[0]
    assert "sex" not in rig["dynamo"].rows("players")[0]


def test_legacy_player_supplies_phone_after_verification_without_new_participation(rig):
    enable_profile(rig)
    original = seed_returning_player(rig, phone=None, jobTitle="Engineer")
    details = response_body(resume_request())
    assert details["detailsRequired"]
    assert details["fields"]["phone"]
    challenge = response_body(resume_request(phone="+34600000000", company="ACME", sex="male"))
    assert rig["dynamo"].rows("players")[0]["phone"] is None
    result = join(rig, challenge["challengeId"], sent_code(rig))
    assert result["statusCode"] == 200
    players = rig["dynamo"].rows("players")
    assert len(players) == 1
    assert players[0]["playerId"] == original["playerId"]
    assert players[0]["phone"] == "+34600000000"
    assert players[0]["company"] == "ACME"
    assert players[0]["sex"] == "male"
    assert players[0]["jobTitle"] == "Engineer"
    assert players[0]["type"] == original["type"]


def test_optional_fields_never_force_returning_players_to_complete_profile(rig):
    enable_profile(rig)
    seed_returning_player(rig)
    assert "challengeId" in response_body(resume_request())


@pytest.mark.parametrize("sex", ["male", "female", " male ", "", "  ", None])
def test_sex_is_independent_optional_and_bound_to_verification(rig, sex):
    enable_profile(rig)
    challenge = response_body(request_code(sex=sex))
    assert join(rig, challenge["challengeId"], sent_code(rig))["statusCode"] == 200
    player = rig["dynamo"].rows("players")[0]
    assert player.get("sex") == (sex.strip() if sex and sex.strip() else None)
    assert "jobTitle" not in player


def test_sex_configuration_preserves_legacy_job_title_without_converting_it():
    legacy = normalize_participant_fields("T1MER", {"jobTitle": {"enabled": True}}, strict=True)
    assert legacy["jobTitle"] == {"enabled": True}
    assert legacy["sex"] == {"enabled": False}
    configured = normalize_participant_fields("T1MER", {"sex": {"enabled": True}}, strict=True)
    assert configured["sex"] == {"enabled": True}
    assert configured["jobTitle"] == {"enabled": False}
    with pytest.raises(ValueError, match="enabled must be a boolean"):
        normalize_participant_fields("T1MER", {"sex": {"enabled": "true"}}, strict=True)
