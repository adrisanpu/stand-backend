"""Format the shared, configurable SES sender."""
from email.utils import formataddr


def format_sender(address, name):
    if not isinstance(name, str) or len(name) > 100 or any(
        ord(char) < 32 or 127 <= ord(char) <= 159 for char in name
    ):
        raise ValueError("InvalidEmailSenderName")
    name = name.strip()
    return formataddr((name, address), charset="utf-8") if name else address
