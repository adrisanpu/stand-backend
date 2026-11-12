import base64
import hashlib
import json
from datetime import datetime, timedelta, timezone
from io import BytesIO
from urllib.parse import urlsplit
from zipfile import ZipFile

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import NameOID

from stand_common import wallet_passes


AWARD = {
    "id": "award_abc",
    "gameId": "ABC123",
    "playerId": 7,
    "name": "Premio",
    "gameName": "Evento",
}


def _b64url_decode(value):
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def test_google_save_url_has_signed_award_and_barcode():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    url = wallet_passes.build_google_save_url(AWARD, "claim-token", {
        "issuerId": "123456789",
        "classId": "stand-awards",
        "serviceAccountEmail": "wallet@project.iam.gserviceaccount.com",
        "privateKeyPem": pem,
    })
    parts = urlsplit(url).path.split("/")
    assert url.startswith("https://pay.google.com/gp/v/save/")
    jwt = parts[-1]
    header, payload, signature = jwt.split(".")
    signing_input = f"{header}.{payload}".encode("ascii")
    key.public_key().verify(_b64url_decode(signature), signing_input, padding.PKCS1v15(), hashes.SHA256())
    claims = json.loads(_b64url_decode(payload))
    object_data = claims["payload"]["genericObjects"][0]
    assert object_data["classId"] == "123456789.stand-awards"
    assert object_data["barcode"]["value"] == "claim-token"
    assert object_data["header"]["defaultValue"]["value"] == "Premio"


def test_apple_pass_contains_manifest_and_signed_barcode():
    now = datetime.now(timezone.utc)
    root_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    root_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "STAND WWDR test")])
    root_cert = (
        x509.CertificateBuilder().subject_name(root_name).issuer_name(root_name)
        .public_key(root_key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=365))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(root_key, hashes.SHA256())
    )
    leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    leaf_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "STAND Pass")]))
        .issuer_name(root_name).public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=365))
        .sign(root_key, hashes.SHA256())
    )
    p12 = pkcs12.serialize_key_and_certificates(
        b"stand", leaf_key, leaf_cert, [root_cert], serialization.NoEncryption(),
    )
    credentials = {
        "certificateP12Base64": base64.b64encode(p12).decode("ascii"),
        "wwdrCertificatePem": root_cert.public_bytes(serialization.Encoding.PEM).decode(),
        "passTypeIdentifier": "pass.example.stand",
        "teamIdentifier": "TEAM123",
    }

    content = wallet_passes.build_apple_pass(AWARD, "claim-token", credentials)
    with ZipFile(BytesIO(content)) as archive:
        names = set(archive.namelist())
        assert {"pass.json", "manifest.json", "signature", "icon.png", "icon@2x.png", "icon@3x.png"} <= names
        manifest = json.loads(archive.read("manifest.json"))
        for name, digest in manifest.items():
            assert hashlib.sha1(archive.read(name)).hexdigest() == digest
        pass_data = json.loads(archive.read("pass.json"))
        assert pass_data["passTypeIdentifier"] == "pass.example.stand"
        assert pass_data["barcodes"][0]["message"] == "claim-token"


def test_wallet_pass_rejects_control_characters_in_award():
    with pytest.raises(wallet_passes.WalletPassError, match="InvalidWalletAward"):
        wallet_passes._pass_files({**AWARD, "name": "Bad\nPrize"}, "token", {
            "passTypeIdentifier": "pass.example.stand",
            "teamIdentifier": "TEAM123",
        })
