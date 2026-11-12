"""Apple Wallet and Google Wallet pass construction for STAND awards."""
import base64
import hashlib
import json
import struct
import time
import zlib
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

from cryptography import x509
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import pkcs12, pkcs7


class WalletPassError(Exception):
    pass


def _award_value(award, name, maximum):
    value = award.get(name)
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise WalletPassError("InvalidWalletAward")
    if any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in value):
        raise WalletPassError("InvalidWalletAward")
    return value.strip()


def _pass_identity(award):
    identity = f"{award.get('gameId')}:{award.get('id')}:{award.get('playerId')}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _png_chunk(kind, data):
    return (struct.pack(">I", len(data)) + kind + data
            + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF))


def _icon_png(size):
    scale = size // 29
    pattern = (
        "11111", "10000", "10000", "10000", "11110", "00001", "00001",
        "00001", "11111",
    )
    pixels = []
    for y in range(size):
        row = bytearray([0])
        for x in range(size):
            px, py = x // scale, y // scale
            mark = (7 <= px < 22 and 10 <= py < 19
                    and pattern[py - 10][(px - 7) // 3] == "1")
            row.extend((24, 35, 44) if not mark else (245, 196, 81))
        pixels.append(bytes(row))
    raw = b"".join(pixels)
    header = struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0)
    return (b"\x89PNG\r\n\x1a\n" + _png_chunk(b"IHDR", header)
            + _png_chunk(b"IDAT", zlib.compress(raw, 9)) + _png_chunk(b"IEND", b""))


def _pass_files(award, token, credentials):
    name = _award_value(award, "name", 200)
    game_name = award.get("gameName")
    if not isinstance(game_name, str) or not game_name.strip():
        game_name = "STAND"
    return {
        "pass.json": json.dumps({
            "formatVersion": 1,
            "passTypeIdentifier": credentials["passTypeIdentifier"],
            "serialNumber": _pass_identity(award),
            "teamIdentifier": credentials["teamIdentifier"],
            "organizationName": credentials.get("organizationName", "STAND"),
            "description": "Premio STAND",
            "logoText": "STAND",
            "backgroundColor": "rgb(24, 35, 44)",
            "foregroundColor": "rgb(255, 255, 255)",
            "labelColor": "rgb(245, 196, 81)",
            "generic": {
                "primaryFields": [{"key": "prize", "label": "PREMIO", "value": name}],
                "secondaryFields": [{"key": "event", "label": "EVENTO", "value": game_name}],
            },
            "barcodes": [{
                "format": "PKBarcodeFormatQR",
                "message": token,
                "messageEncoding": "iso-8859-1",
            }],
        }, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        "icon.png": _icon_png(29),
        "icon@2x.png": _icon_png(58),
        "icon@3x.png": _icon_png(87),
    }


def build_apple_pass(award, token, credentials):
    try:
        p12_data = base64.b64decode(credentials["certificateP12Base64"], validate=True)
        password = credentials.get("certificatePassword")
        key, certificate, chain = pkcs12.load_key_and_certificates(
            p12_data, password.encode("utf-8") if isinstance(password, str) and password else None,
        )
        wwdr = x509.load_pem_x509_certificate(credentials["wwdrCertificatePem"].encode("utf-8"))
        if key is None or certificate is None:
            raise WalletPassError("WalletPassUnavailable")
        files = _pass_files(award, token, credentials)
        manifest = {name: hashlib.sha1(content).hexdigest() for name, content in files.items()}
        manifest_bytes = json.dumps(manifest, separators=(",", ":")).encode("utf-8")
        signature = (pkcs7.PKCS7SignatureBuilder().set_data(manifest_bytes)
                     .add_signer(certificate, key, hashes.SHA256())
                     .add_certificate(wwdr)
                     .sign(serialization.Encoding.DER, [
                         pkcs7.PKCS7Options.DetachedSignature,
                         pkcs7.PKCS7Options.Binary,
                     ]))
        output = BytesIO()
        with ZipFile(output, "w", ZIP_DEFLATED) as archive:
            for name, content in files.items():
                archive.writestr(name, content)
            archive.writestr("manifest.json", manifest_bytes)
            archive.writestr("signature", signature)
        return output.getvalue()
    except WalletPassError:
        raise
    except (KeyError, TypeError, ValueError, UnsupportedAlgorithm) as exc:
        raise WalletPassError("WalletPassUnavailable") from exc


def build_google_save_url(award, token, credentials):
    try:
        issuer = str(credentials["issuerId"])
        class_id = str(credentials["classId"])
        service_account = credentials["serviceAccountEmail"]
        private_key = credentials["privateKeyPem"].encode("utf-8")
        if not issuer.isdigit() or not service_account.endswith(".iam.gserviceaccount.com"):
            raise ValueError("InvalidWalletCredentials")
        full_class_id = class_id if class_id.startswith(f"{issuer}.") else f"{issuer}.{class_id}"
        object_id = f"{issuer}.stand_{_pass_identity(award)}"
        name = _award_value(award, "name", 200)
        private_key_obj = serialization.load_pem_private_key(private_key, password=None)
        header = {"alg": "RS256", "typ": "JWT"}
        now = int(time.time())
        payload = {
            "iss": service_account,
            "aud": "google",
            "typ": "savetowallet",
            "iat": now,
            "payload": {
                "genericObjects": [{
                    "id": object_id,
                    "classId": full_class_id,
                    "state": "ACTIVE",
                    "cardTitle": {"defaultValue": {"language": "es", "value": "STAND"}},
                    "header": {"defaultValue": {"language": "es", "value": name}},
                    "textModulesData": [{
                        "header": "Premio",
                        "body": "Muestra el código de tu tarjeta al equipo del evento.",
                    }],
                    "barcode": {"type": "QR_CODE", "value": token},
                }],
            },
        }
        encode = lambda value: base64.urlsafe_b64encode(
            json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        ).rstrip(b"=").decode("ascii")
        signing_input = f"{encode(header)}.{encode(payload)}".encode("ascii")
        signature = private_key_obj.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
        jwt = signing_input.decode("ascii") + "." + base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii")
        return f"https://pay.google.com/gp/v/save/{jwt}"
    except WalletPassError:
        raise
    except (KeyError, TypeError, ValueError, AttributeError, UnsupportedAlgorithm) as exc:
        raise WalletPassError("WalletPassUnavailable") from exc
