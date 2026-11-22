from email.header import decode_header, make_header
from email.utils import parseaddr

import pytest

from stand_common.email_sender import format_sender


@pytest.mark.parametrize("name", ["", "   ", "STAND", "STAND, premios", "Premios España"])
def test_sender_round_trips_name_and_address(name):
    header = format_sender("sender@example.test", name)
    parsed_name, address = parseaddr(header)
    assert str(make_header(decode_header(parsed_name))) == name.strip()
    assert address == "sender@example.test"
    if not name.strip():
        assert header == "sender@example.test"
    assert header.isascii()


@pytest.mark.parametrize("name", ["STAND\r\nBcc: other@example.test", "Bad\x7fName", "x" * 101, None])
def test_sender_rejects_invalid_name(name):
    with pytest.raises(ValueError, match="InvalidEmailSenderName"):
        format_sender("sender@example.test", name)
