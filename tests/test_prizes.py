import binascii
import struct
import zlib

import pytest

from stand_common.prizes import (
    MAX_PRIZE_IMAGE_BYTES,
    allowed_prize_slots,
    serialize_prize_catalog,
    validate_png,
    validate_prize_catalog,
)


def _chunk(name, contents):
    raw = name + contents
    return struct.pack(">I", len(contents)) + raw + struct.pack(">I", binascii.crc32(raw) & 0xFFFFFFFF)


def _png(width=1, height=1, color_type=6, bit_depth=8, scanlines=b"\x00\x00\x00\x00\xff",
         compressed_data=None):
    ihdr = struct.pack(">IIBBBBB", width, height, bit_depth, color_type, 0, 0, 0)
    return (
        b"\x89PNG\r\n\x1a\n"
        + _chunk(b"IHDR", ihdr)
        + _chunk(b"IDAT", compressed_data if compressed_data is not None else zlib.compress(scanlines))
        + _chunk(b"IEND", b"")
    )


def test_validates_png_signature_crc_and_decoded_scanlines():
    validate_png(_png())

    with pytest.raises(ValueError, match="signature"):
        validate_png(b"not a png")
    corrupt_crc = bytearray(_png())
    corrupt_crc[29] ^= 1
    with pytest.raises(ValueError, match="checksum"):
        validate_png(bytes(corrupt_crc))
    with pytest.raises(ValueError, match="cannot be decoded"):
        validate_png(_png(compressed_data=b"not compressed"))
    with pytest.raises(ValueError, match="invalid length"):
        validate_png(_png(scanlines=b"not compressed"))
    with pytest.raises(ValueError, match="filter"):
        validate_png(_png(scanlines=b"\x05\x00\x00\x00\xff"))


def test_png_rejects_incomplete_and_oversized_images():
    with pytest.raises(ValueError, match="incomplete"):
        validate_png(_png()[:-12])
    with pytest.raises(ValueError, match="5 MB"):
        validate_png(b"\x89PNG\r\n\x1a\n" + b"x" * (MAX_PRIZE_IMAGE_BYTES + 1))


def test_prize_catalog_slot_matrix_and_optional_catalog_compatibility():
    assert allowed_prize_slots("EMPAREJA2", False) == {"validation"}
    assert allowed_prize_slots("T1MER", False) == {"ranking"}
    assert allowed_prize_slots("STACK", True) == {"ranking", "raffle"}
    assert allowed_prize_slots("L3TRAS", True) == {"validation", "raffle"}

    catalog = validate_prize_catalog(
        {"validation": {"name": "Premio", "limit": None}},
        "EMPAREJA2",
        False,
        "owner",
        "ABC123",
    )
    assert catalog == {
        "validation": {"name": "Premio", "limit": None, "imageKey": None},
    }
    assert serialize_prize_catalog(catalog, "owner", "ABC123", "", lambda **_: None) == {
        "validation": {"name": "Premio", "limit": None, "imageKey": None, "imageUrl": None},
    }


@pytest.mark.parametrize("catalog,game_type,raffle,match", [
    ({"ranking": {"name": "Prize", "limit": 1}}, "EMPAREJA2", False, "not supported"),
    ({"raffle": {"name": "Prize", "limit": 1}}, "T1MER", False, "not supported"),
    ({"validation": {"name": "Prize", "limit": True}}, "EMPAREJA2", False, "non-negative integer"),
    ({"ranking": {"name": "Prize", "limit": 1, "topX": 0}}, "T1MER", False, "topX"),
    ({"ranking": {"name": "Prize", "limit": 1, "topX": 101}}, "T1MER", False, "topX"),
    ({"ranking": {"name": "Prize", "limit": 1, "imageUrl": "https://wrong"}}, "T1MER", False, "unsupported fields"),
    ({"validation": {"name": 1, "limit": 1}}, "EMPAREJA2", False, "name"),
])
def test_rejects_invalid_prize_catalog(catalog, game_type, raffle, match):
    with pytest.raises(ValueError, match=match):
        validate_prize_catalog(catalog, game_type, raffle, "owner", "ABC123")


def test_ranking_top_x_and_owner_bound_image_key():
    key = "games/owner/ABC123/ranking/01234567-89ab-cdef-0123-456789abcdef.png"
    catalog = validate_prize_catalog(
        {"ranking": {"name": "Winner", "limit": 2, "topX": 5, "imageKey": key}},
        "T1MER",
        False,
        "owner",
        "ABC123",
    )
    signed = serialize_prize_catalog(
        catalog,
        "owner",
        "ABC123",
        "bucket",
        lambda operation, **kwargs: "signed:" + kwargs["Params"]["Key"],
    )
    assert signed["ranking"]["imageUrl"] == "signed:" + key
    with pytest.raises(ValueError, match="valid uploaded image"):
        validate_prize_catalog(
            {"ranking": {"name": "Winner", "limit": 2, "imageKey": key}},
            "T1MER",
            False,
            "another-owner",
            "ABC123",
        )
    with pytest.raises(ValueError, match="Stored ranking prize image key"):
        serialize_prize_catalog(
            catalog,
            "wrong-owner",
            "ABC123",
            "bucket",
            lambda *_args, **_kwargs: "signed",
        )


def test_prize_name_is_persisted_as_the_supplied_string():
    catalog = validate_prize_catalog(
        {"validation": {"name": "", "limit": None}},
        "EMPAREJA2",
        False,
        "owner",
        "ABC123",
    )
    assert catalog["validation"]["name"] == ""
