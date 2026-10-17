"""Shared prize-catalog validation, PNG verification, and read serialization."""
import binascii
import math
import re
import struct
import zlib


MAX_PRIZE_IMAGE_BYTES = 5 * 1024 * 1024
MAX_PNG_DECODED_BYTES = 64 * 1024 * 1024
PRIZE_IMAGE_URL_TTL = 3600

VALIDATION_PRIZE_GAMES = frozenset({"EMPAREJA2", "CUPIDO", "SEMAFORO", "L3TRAS"})
RANKING_PRIZE_GAMES = frozenset({"T1MER", "STACK"})
PRIZE_SLOTS = frozenset({"validation", "ranking", "raffle"})
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_PRIZE_KEY_RE = re.compile(
    r"^games/([^/]+)/([A-Z0-9]{6})/(validation|ranking|raffle)/"
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.png$"
)
_STAGED_PRIZE_KEY_RE = re.compile(
    r"^games/staged/([^/]+)/([A-Z0-9]{6})/(validation|ranking|raffle)/"
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.png$"
)


def allowed_prize_slots(game_type: str, raffles_enabled: bool) -> set[str]:
    game_type = (game_type or "").upper()
    slots = set()
    if game_type in VALIDATION_PRIZE_GAMES:
        slots.add("validation")
    if game_type in RANKING_PRIZE_GAMES:
        slots.add("ranking")
    if raffles_enabled:
        slots.add("raffle")
    return slots


def prize_image_key(owner_id: str, game_id: str, slot: str, image_key: str) -> bool:
    """Return whether a key is an immutable committed image for this owner/game/slot."""
    match = _PRIZE_KEY_RE.fullmatch(image_key) if isinstance(image_key, str) else None
    return bool(match and match.group(1) == owner_id and match.group(2) == game_id
                and match.group(3) == slot)


def staged_prize_image_key(owner_id: str, game_id: str, slot: str, image_key: str) -> bool:
    """Return whether a key is a staged upload for this exact owner/game/slot."""
    match = _STAGED_PRIZE_KEY_RE.fullmatch(image_key) if isinstance(image_key, str) else None
    return bool(match and match.group(1) == owner_id and match.group(2) == game_id
                and match.group(3) == slot)


def validate_prize_catalog(catalog, game_type: str, raffles_enabled: bool,
                           owner_id: str, game_id: str,
                           existing_catalog: dict | None = None) -> dict:
    """Validate a catalog; omitted imageKey keeps its existing association, null removes it."""
    if not isinstance(catalog, dict):
        raise ValueError("prizeCatalog must be an object.")
    allowed = allowed_prize_slots(game_type, raffles_enabled)
    unknown = set(catalog) - PRIZE_SLOTS
    if unknown:
        raise ValueError(f"Unsupported prize slot: {sorted(unknown)[0]}.")
    existing_catalog = existing_catalog if isinstance(existing_catalog, dict) else {}
    if (
        "raffle" in existing_catalog
        and "raffle" not in allowed
        and "raffle" not in catalog
    ):
        raise ValueError("Existing raffle prize must be preserved when the current plan does not allow raffle edits.")
    normalized = {}
    for slot, prize in catalog.items():
        if not isinstance(prize, dict):
            raise ValueError(f"prizeCatalog.{slot} must be an object.")
        allowed_fields = {"name", "limit", "imageKey"}
        if slot == "ranking":
            allowed_fields.add("topX")
        if set(prize) - allowed_fields:
            raise ValueError(f"prizeCatalog.{slot} contains unsupported fields.")
        name = prize.get("name")
        if not isinstance(name, str):
            raise ValueError(f"prizeCatalog.{slot}.name must be a string.")
        limit = prize.get("limit")
        if limit is not None and (type(limit) is not int or limit < 0):
            raise ValueError(f"prizeCatalog.{slot}.limit must be a non-negative integer or null.")
        existing_prize = existing_catalog.get(slot) or {}
        image_key = prize.get("imageKey", existing_prize.get("imageKey"))
        key_is_committed = image_key is None or prize_image_key(owner_id, game_id, slot, image_key)
        key_is_staged = image_key is not None and staged_prize_image_key(
            owner_id, game_id, slot, image_key,
        )
        if image_key is not None and not (key_is_committed or key_is_staged):
            raise ValueError(f"prizeCatalog.{slot}.imageKey is not a valid uploaded image for this game.")
        normalized_prize = {"name": name, "limit": limit, "imageKey": image_key}
        if slot == "ranking":
            if "topX" in prize:
                top_x = prize["topX"]
                if type(top_x) is not int or not 1 <= top_x <= 100:
                    raise ValueError("prizeCatalog.ranking.topX must be an integer from 1 to 100.")
                normalized_prize["topX"] = top_x
        elif "topX" in prize:
            raise ValueError(f"prizeCatalog.{slot}.topX is only valid for ranking.")
        if slot not in allowed:
            if slot != "raffle" or not isinstance(existing_catalog.get(slot), dict):
                raise ValueError(f"The {slot} prize is not supported for this game or plan.")
            previous = existing_catalog[slot]
            previous_prize = {
                "name": previous.get("name"),
                "limit": previous.get("limit"),
                "imageKey": previous.get("imageKey"),
            }
            if normalized_prize != previous_prize or key_is_staged:
                raise ValueError("Existing raffle prize cannot be changed without the required plan.")
        normalized[slot] = normalized_prize
    return normalized


def _paeth(left: int, above: int, upper_left: int) -> int:
    estimate = left + above - upper_left
    left_distance = abs(estimate - left)
    above_distance = abs(estimate - above)
    upper_left_distance = abs(estimate - upper_left)
    if left_distance <= above_distance and left_distance <= upper_left_distance:
        return left
    if above_distance <= upper_left_distance:
        return above
    return upper_left


def _validate_scanlines(decoded: bytes, passes: list[tuple[int, int]], bits_per_pixel: int,
                        color_type: int, palette_entries: int) -> None:
    offset = 0
    bytes_per_pixel = max(1, math.ceil(bits_per_pixel / 8))
    for pass_width, pass_height in passes:
        row_bytes = math.ceil(pass_width * bits_per_pixel / 8)
        previous = bytearray(row_bytes)
        for _ in range(pass_height):
            filter_type = decoded[offset]
            raw = decoded[offset + 1:offset + 1 + row_bytes]
            offset += row_bytes + 1
            if filter_type > 4:
                raise ValueError("Invalid PNG filter type.")
            current = bytearray(row_bytes)
            for index, value in enumerate(raw):
                left = current[index - bytes_per_pixel] if index >= bytes_per_pixel else 0
                above = previous[index]
                upper_left = previous[index - bytes_per_pixel] if index >= bytes_per_pixel else 0
                if filter_type == 0:
                    predictor = 0
                elif filter_type == 1:
                    predictor = left
                elif filter_type == 2:
                    predictor = above
                elif filter_type == 3:
                    predictor = (left + above) // 2
                else:
                    predictor = _paeth(left, above, upper_left)
                current[index] = (value + predictor) & 0xFF
            if color_type == 3:
                depth = bits_per_pixel
                mask = (1 << depth) - 1
                for pixel in range(pass_width):
                    bit_offset = pixel * depth
                    shift = 8 - depth - (bit_offset % 8)
                    palette_index = (current[bit_offset // 8] >> shift) & mask
                    if palette_index >= palette_entries:
                        raise ValueError("PNG palette index is out of range.")
            previous = current
    if offset != len(decoded):
        raise ValueError("PNG decoded data has an invalid length.")


def validate_png(data: bytes) -> None:
    """Validate PNG chunks and bounded-decode all scanlines without an image dependency."""
    if not isinstance(data, bytes) or len(data) > MAX_PRIZE_IMAGE_BYTES or len(data) < 8:
        raise ValueError("Prize image must be a PNG no larger than 5 MB.")
    if data[:8] != _PNG_SIGNATURE:
        raise ValueError("Prize image does not have a PNG signature.")

    offset = 8
    chunk_count = 0
    width = height = bit_depth = color_type = interlace = None
    palette_entries = 0
    idat_parts = []
    seen_idat = False
    ended_idat = False
    seen_palette = False
    seen_iend = False
    while offset < len(data):
        chunk_count += 1
        if chunk_count > 4096 or offset + 12 > len(data):
            raise ValueError("PNG has an invalid chunk structure.")
        length = struct.unpack(">I", data[offset:offset + 4])[0]
        chunk_type = data[offset + 4:offset + 8]
        end = offset + 12 + length
        if end > len(data) or any(not (65 <= byte <= 90 or 97 <= byte <= 122) for byte in chunk_type):
            raise ValueError("PNG has an invalid chunk structure.")
        chunk_data = data[offset + 8:offset + 8 + length]
        expected_crc = struct.unpack(">I", data[offset + 8 + length:end])[0]
        actual_crc = binascii.crc32(chunk_type + chunk_data) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise ValueError("PNG chunk checksum is invalid.")

        if chunk_count == 1 and chunk_type != b"IHDR":
            raise ValueError("PNG must start with IHDR.")
        if chunk_type == b"IHDR":
            if chunk_count != 1 or length != 13:
                raise ValueError("PNG IHDR is invalid.")
            width, height, bit_depth, color_type, compression, filtering, interlace = struct.unpack(
                ">IIBBBBB", chunk_data,
            )
            valid_depths = {
                0: {1, 2, 4, 8, 16},
                2: {8, 16},
                3: {1, 2, 4, 8},
                4: {8, 16},
                6: {8, 16},
            }
            if (width < 1 or height < 1 or width > 8192 or height > 8192
                    or width * height > 40_000_000 or color_type not in valid_depths
                    or bit_depth not in valid_depths[color_type]
                    or compression != 0 or filtering != 0 or interlace not in (0, 1)):
                raise ValueError("PNG image dimensions or IHDR fields are invalid.")
        elif chunk_type == b"PLTE":
            if seen_palette or seen_idat or length == 0 or length > 768 or length % 3:
                raise ValueError("PNG palette is invalid.")
            if color_type in (0, 4):
                raise ValueError("PNG palette is not valid for this color type.")
            palette_entries = length // 3
            if color_type == 3 and palette_entries > (1 << bit_depth):
                raise ValueError("PNG palette has too many entries.")
            seen_palette = True
        elif chunk_type == b"IDAT":
            if ended_idat or (color_type == 3 and not seen_palette):
                raise ValueError("PNG image data chunks are invalid.")
            seen_idat = True
            idat_parts.append(chunk_data)
        else:
            if seen_idat:
                ended_idat = True
            if chunk_type == b"IEND":
                if length != 0 or not seen_idat:
                    raise ValueError("PNG IEND is invalid.")
                seen_iend = True
                offset = end
                break
            if chunk_type[0] & 0x20 == 0 and chunk_type not in (b"IHDR", b"PLTE"):
                raise ValueError("PNG contains an unknown critical chunk.")
            if chunk_type == b"PLTE":
                raise ValueError("PNG palette is misplaced.")
        offset = end

    if not seen_iend or offset != len(data) or not idat_parts or not any(idat_parts):
        raise ValueError("PNG is incomplete.")
    channels = {0: 1, 2: 3, 3: 1, 4: 2, 6: 4}[color_type]
    bits_per_pixel = channels * bit_depth
    if interlace:
        adam7 = ((0, 0, 8, 8), (4, 0, 8, 8), (0, 4, 4, 8), (2, 0, 4, 4),
                 (0, 2, 2, 4), (1, 0, 2, 2), (0, 1, 1, 2))
        passes = []
        for x_start, y_start, x_step, y_step in adam7:
            pass_width = max(0, (width - x_start + x_step - 1) // x_step)
            pass_height = max(0, (height - y_start + y_step - 1) // y_step)
            if pass_width and pass_height:
                passes.append((pass_width, pass_height))
    else:
        passes = [(width, height)]
    expected_size = sum(
        pass_height * (1 + math.ceil(pass_width * bits_per_pixel / 8))
        for pass_width, pass_height in passes
    )
    if expected_size < 1 or expected_size > MAX_PNG_DECODED_BYTES:
        raise ValueError("PNG decoded image is too large.")

    decoder = zlib.decompressobj()
    compressed = b"".join(idat_parts)
    decoded = bytearray()
    pending = compressed
    try:
        while pending:
            remaining = expected_size + 1 - len(decoded)
            if remaining <= 0:
                raise ValueError("PNG decoded data is too large.")
            decoded.extend(decoder.decompress(pending, remaining))
            if len(decoded) > expected_size:
                raise ValueError("PNG decoded data has an invalid length.")
            pending = decoder.unconsumed_tail
            if not pending:
                break
    except zlib.error as exc:
        raise ValueError("PNG image data cannot be decoded.") from exc
    if not decoder.eof or decoder.unused_data or len(decoded) != expected_size:
        raise ValueError("PNG image data cannot be decoded.")
    _validate_scanlines(bytes(decoded), passes, bits_per_pixel, color_type, palette_entries)


def serialize_prize_catalog(catalog, owner_id: str, game_id: str, bucket: str,
                            presign_get) -> dict:
    """Return the durable catalog plus fresh signed GET URLs; absent legacy catalogs stay absent."""
    if not isinstance(catalog, dict):
        raise ValueError("Stored prizeCatalog must be an object.")
    output = {}
    for slot, prize in catalog.items():
        if slot not in PRIZE_SLOTS or not isinstance(prize, dict):
            raise ValueError("Stored prizeCatalog contains an invalid slot.")
        image_key = prize.get("imageKey")
        image_url = None
        if image_key is not None:
            if not prize_image_key(owner_id, game_id, slot, image_key):
                raise ValueError(f"Stored {slot} prize image key is invalid.")
            if not bucket:
                raise RuntimeError("Missing USER_MEDIA_BUCKET for configured prize image.")
            image_url = presign_get(
                "get_object",
                Params={"Bucket": bucket, "Key": image_key},
                ExpiresIn=PRIZE_IMAGE_URL_TTL,
            )
            if not image_url:
                raise RuntimeError(f"Could not create a read URL for the {slot} prize image.")
        output[slot] = {**prize, "imageUrl": image_url}
    return output
