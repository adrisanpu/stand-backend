from copy import deepcopy

import pytest

from stand_common import shared_prizes, stack_prizes


class MemoryTable:
    def __init__(self, items, key_name):
        self.items = {item[key_name]: deepcopy(item) for item in items}
        self.key_name = key_name

    def get_item(self, Key, **_kwargs):
        item = self.items.get(Key[self.key_name])
        return {"Item": deepcopy(item)} if item else {}

    def query(self, **_kwargs):
        return {"Items": deepcopy(list(self.items.values()))}

    def update_item(self, Key, UpdateExpression, ExpressionAttributeValues, **_kwargs):
        item = self.items[Key[self.key_name]]
        values = ExpressionAttributeValues
        if self.key_name == "userId":
            for field, marker in (
                ("sharedPrizeCatalog", ":catalog"),
                ("sharedPrizeCatalogRevision", ":revision"),
                ("sharedPrizeCatalogState", ":pending"),
                ("sharedPrizeCatalogState", ":ready"),
            ):
                if marker in values:
                    item[field] = deepcopy(values[marker])
        else:
            for field, marker in (
                ("prizeCatalog", ":catalog"),
                ("stackPrizeProgram", ":program"),
                ("prizeCatalog", ":prizeCatalog"),
                ("stackPrizeProgram", ":stackPrizeProgram"),
            ):
                if marker in values:
                    item[field] = deepcopy(values[marker])


def test_migration_merges_exact_name_and_image_pairs_and_links_existing_assignments():
    games = MemoryTable([
        {
            "gameId": "ABC123", "ownerUserId": "owner",
            "prizeCatalog": {"ranking": {
                "name": "Gift", "imageKey": "games/owner/ABC123/ranking/gift.png",
                "limit": 5, "topX": 4,
            }},
            "stackPrizeProgram": {"catalog": [{
                "id": "stand-one", "source": "stand", "name": "Gift",
                "imageKey": "games/owner/ABC123/ranking/gift.png", "stockTotal": 40,
            }]},
        },
        {
            "gameId": "DEF456", "ownerUserId": "owner",
            "prizeCatalog": {"validation": {
                "name": "Gift", "imageKey": "games/owner/DEF456/validation/other.png",
            }},
        },
    ], "gameId")
    users = MemoryTable([{"userId": "owner"}], "userId")

    catalog = shared_prizes.migrate(users, games, "owner")

    assert len(catalog) == 2
    by_identity = {(item["name"], item["imageKey"]): item["id"] for item in catalog}
    assert games.items["ABC123"]["prizeCatalog"]["ranking"]["sharedPrizeId"] == by_identity[
        ("Gift", "games/owner/ABC123/ranking/gift.png")
    ]
    assert games.items["ABC123"]["stackPrizeProgram"]["catalog"][0]["sharedPrizeId"] == by_identity[
        ("Gift", "games/owner/ABC123/ranking/gift.png")
    ]
    assert games.items["ABC123"]["prizeCatalog"]["ranking"]["limit"] == 5
    assert games.items["ABC123"]["prizeCatalog"]["ranking"]["topX"] == 4
    assert users.items["owner"]["sharedPrizeCatalogState"] == "ready"
    assert users.items["owner"]["sharedPrizeCatalogRevision"] == 1


def test_synchronize_propagates_shared_details_without_changing_game_stock_or_limits():
    games = MemoryTable([{
        "gameId": "ABC123", "ownerUserId": "owner", "stackPrizeStock": {"prize-one": {"remaining": 7}},
        "prizeCatalog": {"ranking": {
            "sharedPrizeId": "shared-one", "name": "Old", "imageKey": "old.png",
            "limit": 3, "topX": 4,
        }},
        "stackPrizeProgram": {"catalog": [{
            "id": "prize-one", "source": "stand", "sharedPrizeId": "shared-one",
            "name": "Old", "imageKey": "old.png", "stockTotal": 12, "limit": 2,
        }]},
    }], "gameId")

    shared_prizes.synchronize(games, "owner", [{
        "id": "shared-one", "name": "New", "imageKey": "prizes/owner/new.png",
    }])

    game = games.items["ABC123"]
    assert game["prizeCatalog"]["ranking"] == {
        "sharedPrizeId": "shared-one", "name": "New", "imageKey": "prizes/owner/new.png",
        "limit": 3, "topX": 4,
    }
    assert game["stackPrizeProgram"]["catalog"][0] == {
        "id": "prize-one", "source": "stand", "sharedPrizeId": "shared-one",
        "name": "New", "imageKey": "prizes/owner/new.png", "stockTotal": 12, "limit": 2,
    }
    assert game["stackPrizeStock"] == {"prize-one": {"remaining": 7}}


def test_shared_catalog_validation_rejects_duplicate_ids_and_unknown_fields():
    with pytest.raises(ValueError, match="IDs must be unique"):
        shared_prizes.validate([
            {"id": "same", "name": "One", "imageKey": None},
            {"id": "same", "name": "Two", "imageKey": None},
        ])
    with pytest.raises(ValueError, match="Invalid shared prize"):
        shared_prizes.validate([{"id": "one", "name": "Prize", "imageUrl": "https://invalid"}])


def test_legacy_stack_program_carries_migrated_shared_prize_identity():
    program = stack_prizes.legacy_program({
        "prizeCatalog": {"ranking": {
            "name": "Prize", "imageKey": "prizes/owner/prize.png", "sharedPrizeId": "shared-one",
        }},
    })

    assert program["catalog"][0]["sharedPrizeId"] == "shared-one"
    assert program["catalog"][0]["id"] == "legacy_ranking"


def test_synchronize_is_idempotent():
    games = MemoryTable([{
        "gameId": "ABC123", "ownerUserId": "owner",
        "prizeCatalog": {"ranking": {
            "sharedPrizeId": "shared-one", "name": "Prize", "imageKey": "image.png",
        }},
    }], "gameId")
    definitions = [{"id": "shared-one", "name": "Prize", "imageKey": "image.png"}]

    shared_prizes.synchronize(games, "owner", definitions)
    first = deepcopy(games.items["ABC123"])
    shared_prizes.synchronize(games, "owner", definitions)

    assert games.items["ABC123"] == first
