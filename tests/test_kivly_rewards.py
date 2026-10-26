import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from stand_common import kivly_registration as registration, kivly_rewards, kivly_settings
from stand_common.kivly import KivlyClient, KivlyError
from tests.email_fake import FakeDynamoError
from tests.test_kivly_account import account_event, membership_game
from tests.test_public_web import public_mod, SECRET
from stand_common.utils import issue_player_token


@pytest.fixture
def reward_rig(monkeypatch):
    game = {**membership_game(), "gameId": "ABC123", "gameType": "STACK",
            "kivlyReward": {"id": "reward-1", "name": "Coffee", "locationId": "location"}}
    player = {"gameId": "ABC123", "playerId": 7,
              "kivlyMembership": {"customerId": "customer-1", "locationId": "location",
                                  "consentVersion": game["kivly"]["consentVersion"]},
              "type": {"STACK": {"score": 0, "attempt": {"status": "finished"}}}}
    conn = {"locationId": "location", "ownerId": "owner", "version": "v1",
            "clubSettings": {key: game["kivly"][key] for key in ("clubName", "privacyUrl", "termsUrl")}}
    client, table, records = MagicMock(), MagicMock(), {}
    client.list_rewards.return_value = [{"id": "reward-1", "name": "Coffee"}]
    client.customer_rewards.side_effect = [
        [], [{"id": "assignment-1", "customer_id": "customer-1", "reward_id": "reward-1",
              "appleWalletUrl": "https://api.getkivly.com/pass/abc", "googleWalletUrl": "https://pay.google.com/gp/v/save/abc"}],
    ]

    def put(Item, **kwargs):
        if Item["id"] in records:
            raise FakeDynamoError("ConditionalCheckFailedException")
        records[Item["id"]] = deepcopy(Item)

    def update(Key, ExpressionAttributeValues, **kwargs):
        records[Key["id"]].update(state=ExpressionAttributeValues[":s"], redemptionId=ExpressionAttributeValues[":r"])

    table.get_item.side_effect = lambda Key, **_: {"Item": deepcopy(records[Key["id"]])} if Key["id"] in records else {}
    table.put_item.side_effect = put
    table.update_item.side_effect = update
    monkeypatch.setattr(registration, "connection", lambda *_: conn)
    monkeypatch.setattr(registration, "_client", lambda *_: client)
    monkeypatch.setattr(registration, "_table", lambda *_: table)
    return game, player, client, table, records


def test_zero_score_finished_game_receives_reward_and_wallet_links_once(reward_rig):
    game, player, client, table, records = reward_rig
    result = kivly_rewards.completion_reward(MagicMock(), game, player)
    assert result["status"] == "confirmed"
    assert result["name"] == "Coffee"
    assert result["appleWalletUrl"].startswith("https://api.getkivly.com/")
    assert result["googleWalletUrl"].startswith("https://pay.google.com/")
    assert records["reward:ABC123:7"]["redemptionId"] == "assignment-1"
    client.earn_reward.assert_called_once_with("location", "owner", "customer-1", "reward-1")
    client.customer_rewards.side_effect = None
    client.customer_rewards.return_value = [
        {"id": "assignment-1", "reward_id": "reward-1", "customer_id": "customer-1"},
    ]
    game["kivlyReward"] = {"id": "different-reward", "name": "New prize", "locationId": "location"}
    again = kivly_rewards.completion_reward(MagicMock(), game, player)
    assert again["status"] == "confirmed"
    assert again["name"] == "Coffee"
    client.earn_reward.assert_called_once()
    table.put_item.assert_called_once()


@pytest.mark.parametrize("blob", [{}, {"score": 1}, {"score": 1, "attempt": {"status": "started"}}])
def test_unfinished_player_cannot_receive_reward(reward_rig, blob):
    game, player, client, _, _ = reward_rig
    player["type"]["STACK"] = blob
    with pytest.raises(registration.RegistrationError, match="KivlyGameNotFinished"):
        kivly_rewards.completion_reward(MagicMock(), game, player)
    client.earn_reward.assert_not_called()


def test_reward_requires_membership(reward_rig):
    game, player, client, _, _ = reward_rig
    player["kivlyMembership"] = {}
    with pytest.raises(registration.RegistrationError, match="KivlyMembershipRequired"):
        kivly_rewards.completion_reward(MagicMock(), game, player)
    client.earn_reward.assert_not_called()


def test_no_configured_reward_does_not_call_external_api(reward_rig):
    game, player, client, _, _ = reward_rig
    del game["kivlyReward"]
    assert kivly_rewards.completion_reward(MagicMock(), game, player) == {"status": "none"}
    client.earn_reward.assert_not_called()


def test_uncertain_earn_is_pending_and_never_replayed(reward_rig):
    game, player, client, _, _ = reward_rig
    client.earn_reward.side_effect = KivlyError("KivlyUnavailable")
    assert kivly_rewards.completion_reward(MagicMock(), game, player)["status"] == "pending"
    assert kivly_rewards.completion_reward(MagicMock(), game, player)["status"] == "pending"
    client.earn_reward.assert_called_once()


def test_success_response_without_new_assignment_is_not_confirmation(reward_rig):
    game, player, client, _, _ = reward_rig
    client.customer_rewards.side_effect = [[], []]
    assert kivly_rewards.completion_reward(MagicMock(), game, player)["status"] == "pending"
    client.earn_reward.assert_called_once()


def test_ambiguous_new_assignments_stay_pending(reward_rig):
    game, player, client, _, _ = reward_rig
    client.customer_rewards.side_effect = [
        [], [{"id": "assignment-1", "reward_id": "reward-1"},
             {"id": "assignment-2", "reward_id": "reward-1"}],
    ]
    assert kivly_rewards.completion_reward(MagicMock(), game, player)["status"] == "pending"
    assert kivly_rewards.completion_reward(MagicMock(), game, player)["status"] == "pending"
    client.earn_reward.assert_called_once()


def test_existing_customer_is_reused_without_modifying_phone(monkeypatch):
    game = membership_game()
    conn = {"locationId": "location", "ownerId": "owner", "version": "v1"}
    client, table = MagicMock(), MagicMock()
    table.get_item.return_value = {}
    client.find_customer.return_value = {"id": "customer-1", "is_active": True}
    monkeypatch.setattr(registration, "connection", lambda *_: conn)
    monkeypatch.setattr(registration, "_identity", lambda *_: "customer-key")
    monkeypatch.setattr(registration, "_client", lambda *_: client)
    monkeypatch.setattr(registration, "_table", lambda *_: table)
    result = registration.ensure_membership(MagicMock(), game, {
        "email": "test@club.test", "username": "Full Name", "phone": "+34 612345678",
        "loyaltyConsentVersion": game["kivly"]["consentVersion"],
    })
    assert result["customerId"] == "customer-1"
    client.create_customer.assert_not_called()
    assert [call[0] for call in client.method_calls] == ["find_customer"]


def test_conditional_reservation_loser_does_not_earn(reward_rig):
    game, player, client, table, _ = reward_rig
    table.put_item.side_effect = FakeDynamoError("ConditionalCheckFailedException")
    with pytest.raises(registration.RegistrationError, match="KivlyRewardBusy"):
        kivly_rewards.completion_reward(MagicMock(), game, player)
    client.earn_reward.assert_not_called()


@pytest.mark.parametrize("url", ["http://api.getkivly.com/pass", "https://evil.test/pass",
                                 "https://pay.google.com@evil.test/pass", "https://api.getkivly.com:8080/pass"])
def test_invalid_wallet_urls_are_rejected(url):
    with pytest.raises(registration.RegistrationError, match="KivlyWalletInvalid"):
        kivly_rewards.wallet_url(url)


@pytest.mark.parametrize("phone", [None, "+34 600123456"])
def test_customer_creation_includes_phone_only_when_supplied(monkeypatch, phone):
    client = KivlyClient("id", "secret")
    customers = MagicMock(return_value={"id": "customer-1", "email": "test@club.test",
                                       "external_id": "external-1", "is_active": True})
    monkeypatch.setattr(client, "_customers", customers)
    client.create_customer("location", "owner", "Full Name", "test@club.test", "external-1", phone)
    body = customers.call_args.kwargs["customer"]
    assert body["name"] == "Full Name"
    assert body.get("phone") == phone
    assert ("phone" in body) == (phone is not None)


def test_reward_setting_validates_selected_business_catalog(reward_rig):
    game, _, client, _, _ = reward_rig
    games = MagicMock()
    games.get_item.return_value = {"Item": game}
    response = kivly_settings.handle(
        account_event({"gameId": "ABC123", "rewardId": "reward-1"}), "PUT", MagicMock(), games,
    )
    assert response["statusCode"] == 200
    assert games.update_item.call_args.kwargs["ExpressionAttributeValues"][":reward"] == {
        "id": "reward-1", "name": "Coffee", "locationId": "location",
    }
    response = kivly_settings.handle(
        account_event({"gameId": "ABC123", "rewardId": "wrong-business-reward"}), "PUT", MagicMock(), games,
    )
    assert response["statusCode"] == 409
    assert json.loads(response["body"])["error"] == "KivlyRewardUnavailable"


def test_catalog_maps_title_and_uses_skip_pagination(monkeypatch):
    client = KivlyClient("id", "secret")
    request = MagicMock(side_effect=[
        {"data": [{"id": "reward-1", "title": "Coffee", "status": "active"}],
         "total": 2, "page": 1, "limit": 100, "has_next": True, "has_previous": False},
        {"data": [{"id": "reward-2", "title": "Tea", "status": "active"}],
         "total": 2, "page": 2, "limit": 100, "has_next": False, "has_previous": True},
    ])
    monkeypatch.setattr(client, "_scoped_request", request)
    assert client.list_rewards("location", "owner") == [
        {"id": "reward-1", "name": "Coffee"}, {"id": "reward-2", "name": "Tea"},
    ]
    assert "skip=0" in request.call_args_list[0].args[2]
    assert "skip=100" in request.call_args_list[1].args[2]


def test_incomplete_catalog_cannot_be_used_to_select_a_reward(monkeypatch):
    client = KivlyClient("id", "secret")
    monkeypatch.setattr(client, "_scoped_request", MagicMock(return_value={
        "data": [], "total": 1, "page": 1, "limit": 100, "has_next": False, "has_previous": False,
    }))
    with pytest.raises(KivlyError, match="KivlyCatalogInvalid"):
        client.list_rewards("location", "owner")


def test_redemptions_use_array_contract_and_encoded_customer_id(monkeypatch):
    client = KivlyClient("id", "secret")
    request = MagicMock(return_value=[
        {"id": "assignment", "customer_id": "customer/a", "reward_id": "reward-1"},
    ])
    monkeypatch.setattr(client, "_scoped_request", request)
    assert client.customer_rewards("location", "owner", "customer/a")[0]["id"] == "assignment"
    assert request.call_args.args[2] == "/api/v1/redemptions/customer/customer%2Fa?page=1&limit=100"
    assert request.call_args.kwargs == {"response_type": list}


def test_redemptions_for_another_customer_are_rejected(monkeypatch):
    client = KivlyClient("id", "secret")
    monkeypatch.setattr(client, "_scoped_request", MagicMock(return_value=[
        {"id": "assignment", "customer_id": "another-customer", "reward_id": "reward-1"},
    ]))
    with pytest.raises(KivlyError, match="KivlyRewardsInvalid"):
        client.customer_rewards("location", "owner", "customer-1")


def test_earn_only_uses_documented_customer_and_reward_ids(monkeypatch):
    client = KivlyClient("id", "secret")
    request = MagicMock()
    monkeypatch.setattr(client, "_scoped_request", request)
    client.earn_reward("location", "owner", "customer-1", "reward-1")
    request.assert_called_once_with(
        "location", "owner", "/api/v1/redemptions/rewards/earn",
        body={"customer_id": "customer-1", "reward_ids": ["reward-1"]}, response_type=None,
    )


def test_scoped_requests_reject_wrong_location_before_network(monkeypatch):
    client = KivlyClient("id", "secret")
    monkeypatch.setattr(client, "_authenticate", lambda: SimpleNamespace(location_id="different-location", owner_id="owner"))
    request = MagicMock()
    monkeypatch.setattr(client, "_send_request", request)
    with pytest.raises(KivlyError, match="KivlyScopeMismatch"):
        client.list_rewards("location", "owner")
    request.assert_not_called()


def test_settings_catalog_requires_game_owner(reward_rig):
    game, _, client, _, _ = reward_rig
    games = MagicMock()
    games.get_item.return_value = {"Item": {**game, "ownerUserId": "another-owner"}}
    event = account_event()
    event["queryStringParameters"] = {"gameId": "ABC123"}
    response = kivly_settings.handle(event, "GET", MagicMock(), games, catalog=True)
    assert response["statusCode"] == 403
    client.list_rewards.assert_not_called()


def test_reward_selection_can_be_removed(reward_rig):
    game, _, client, _, _ = reward_rig
    games = MagicMock()
    games.get_item.return_value = {"Item": game}
    response = kivly_settings.handle(
        account_event({"gameId": "ABC123", "rewardId": None}), "PUT", MagicMock(), games,
    )
    assert response["statusCode"] == 200
    assert games.update_item.call_args.kwargs["ExpressionAttributeValues"][":reward"] is None
    client.list_rewards.assert_not_called()


def test_public_reward_endpoint_uses_token_player_not_client_claims(reward_rig, monkeypatch):
    game, player, client, _, _ = reward_rig
    monkeypatch.setattr(public_mod, "_get_game", lambda *_: game)
    get_player = MagicMock(return_value=player)
    monkeypatch.setattr(public_mod, "_get_player", get_player)
    response = public_mod.lambda_handler({
        "rawPath": "/v1/public/kivly/reward", "requestContext": {"http": {"method": "POST"}},
        "body": json.dumps({
            "gameId": "ABC123", "playerToken": issue_player_token("ABC123", 7, SECRET),
            "playerId": 999, "rewardId": "untrusted-reward",
        }),
    }, None)
    assert response["statusCode"] == 200
    assert json.loads(response["body"])["reward"]["status"] == "confirmed"
    get_player.assert_called_once_with("ABC123", 7)
    client.earn_reward.assert_called_once_with("location", "owner", "customer-1", "reward-1")


@pytest.mark.parametrize("token", ["invalid-token", issue_player_token("ZZZ999", 7, SECRET)])
def test_public_reward_endpoint_rejects_invalid_or_other_game_token(monkeypatch, token):
    get_game = MagicMock()
    monkeypatch.setattr(public_mod, "_get_game", get_game)
    response = public_mod._handle_kivly_reward({"body": json.dumps({
        "gameId": "ABC123", "playerToken": token,
    })})
    assert response["statusCode"] == 403
    get_game.assert_not_called()
