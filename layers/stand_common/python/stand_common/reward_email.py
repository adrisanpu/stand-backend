"""Reward notice with an inline, locally generated Kivly customer QR."""
from email.message import EmailMessage
from email.policy import SMTP
from html import escape
from io import BytesIO
from urllib.parse import parse_qs, urlsplit

from stand_common.kivly import reward_image_url
from stand_common.utils import is_valid_email, is_valid_phone


def _wallet_link(value):
    from stand_common.stack_prizes import PrizeError, storefront

    try:
        parsed = urlsplit(value)
        storefront(f"{parsed.scheme}://{parsed.netloc}")
        query = parse_qs(parsed.query, strict_parsing=True)
        valid = (
            parsed.path == "/add-to-wallet" and not parsed.fragment
            and set(query) == {"customer_id", "location_id"}
            and all(
                len(values) == 1 and values[0]
                and not any(ord(char) < 33 or 127 <= ord(char) <= 159 for char in values[0])
                for values in query.values()
            )
            and not any(ord(char) < 33 or ord(char) == 127 for char in value)
        )
    except (ValueError, PrizeError):
        raise ValueError("InvalidRewardWalletLink") from None
    if not valid:
        raise ValueError("InvalidRewardWalletLink")
    return value


def build_message(
    sender, recipient, name, prize, phone, *,
    club_name=None, wallet_url=None, logo_png=None,
):
    if not is_valid_email(recipient) or not is_valid_phone(phone):
        raise ValueError("InvalidRewardEmailContact")
    if club_name is not None and (
        not isinstance(club_name, str) or not club_name.strip() or len(club_name) > 200
        or any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in club_name)
    ):
        raise ValueError("InvalidRewardEmailClub")
    if wallet_url is not None:
        if club_name is None or not isinstance(wallet_url, str) or len(wallet_url) > 2048:
            raise ValueError("InvalidRewardWalletLink")
        _wallet_link(wallet_url)
    if logo_png is not None and (
        club_name is None or not isinstance(logo_png, bytes)
        or not logo_png.startswith(b"\x89PNG\r\n\x1a\n")
        or len(logo_png) < 33 or len(logo_png) > 5 * 1024 * 1024
        or logo_png[12:16] != b"IHDR"
    ):
        raise ValueError("InvalidRewardEmailLogo")
    import qrcode
    from qrcode.image.pure import PyPNGImage

    image = BytesIO()
    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M, box_size=8, border=4)
    qr.add_data(phone, optimize=0)
    qr.make(fit=True)
    qr.make_image(image_factory=PyPNGImage).save(image)
    title = prize["name"]
    message = EmailMessage(policy=SMTP)
    message["From"] = sender
    message["To"] = recipient
    message["Subject"] = "Tu premio STAND / Your STAND reward"
    message.set_content(
        f"{name}\n\n{title}\n\n"
        "Tu premio esta asignado a tu cuenta del club. Muestra el QR adjunto al personal para identificarte en Kivly. "
        "No necesitas instalar una wallet.\n"
        "El personal comprobara los premios disponibles y realizara el canje en Kivly. "
        "Este correo no muestra cambios posteriores ni garantiza que el premio siga sin canjear.\n\n"
        "Your reward is assigned to your club account. Show the attached QR to staff to identify yourself in Kivly. "
        "No wallet installation is needed.\n"
        "Staff will check available rewards and redeem them in Kivly. "
        "This email does not reflect later changes or guarantee that the reward is still unredeemed.\n"
    )
    photo = reward_image_url(prize.get("imageUrl"))
    photo_html = f'<img src="{escape(photo, quote=True)}" alt="" width="280">' if photo else ""
    message.add_alternative(
        f'<html><body><h1>{escape(title)}</h1><p>{escape(name)}</p>{photo_html}'
        '<p>Escanea para canjear / Scan to redeem</p>'
        '<img src="cid:reward-qr" width="280" height="280" alt="QR Kivly">'
        '<p>Muestra este QR al personal. No necesitas wallet. El personal comprobara y canjeara el premio en Kivly.</p>'
        '<p>Show this QR to staff. No wallet needed. Staff will check and redeem the reward in Kivly.</p>'
        '<p>Este correo no refleja canjes posteriores. / This email does not reflect subsequent redemptions.</p>'
        '</body></html>', subtype="html",
    )
    if club_name is not None:
        wallet_text = (
            f"A\u00f1ade tu tarjeta a Apple Wallet o Google Wallet:\n{wallet_url}\n\n"
            if wallet_url else ""
        )
        wallet_html = (
            '<table role="presentation" cellpadding="0" cellspacing="0" align="center">'
            '<tr><td bgcolor="#171717" style="border-radius:12px;">'
            f'<a href="{escape(wallet_url, quote=True)}" style="display:inline-block;'
            'padding:16px 24px;color:#ffffff;text-decoration:none;font-size:16px;'
            'font-weight:bold;">A\u00f1adir tarjeta a mi wallet</a></td></tr></table>'
            '<p style="font-size:12px;color:#666666;margin:12px 0 0;">'
            'Apple Wallet o Google Wallet</p>'
            if wallet_url else ""
        )
        logo_html = (
            f'<img src="cid:club-logo" width="220" alt="{escape(club_name, quote=True)}"'
            ' style="display:block;width:220px;max-width:100%;height:auto;margin:0 auto 20px;">'
            if logo_png is not None else ""
        )
        message.replace_header("Subject", f"Tu premio en {club_name}: {title}")
        message.get_payload()[0].set_content(
            f"Hola, {name}:\n\nTu premio en {club_name}: {title}.\n\n"
            "Muestra tu tarjeta de wallet o el QR de este correo al personal del local.\n\n"
            f"{wallet_text}"
            "El QR identifica tu cuenta mediante el tel\u00e9fono registrado en Kivly; "
            "no necesitas instalar una wallet.\n"
            "El personal verificar\u00e1 la disponibilidad y las condiciones antes del canje.\n"
            "Este correo no refleja canjes posteriores.\n"
        )
        message.get_payload()[1].set_content(
            '<!doctype html><html lang="es"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1">'
            '</head><body style="margin:0;padding:0;background-color:#f4f1ed;'
            'font-family:Arial,Helvetica,sans-serif;color:#171717;">'
            '<table role="presentation" width="100%" cellpadding="0" cellspacing="0"'
            ' bgcolor="#f4f1ed"><tr><td align="center" style="padding:28px 12px;">'
            '<table role="presentation" width="100%" cellpadding="0" cellspacing="0"'
            ' style="max-width:600px;background-color:#ffffff;border:1px solid #e8e3dd;'
            'border-radius:20px;"><tr><td align="center" style="padding:32px 24px;">'
            f'{logo_html}<p style="margin:0;font-size:14px;color:#666666;">'
            f'{escape(club_name)}</p>'
            '<h1 style="font-size:26px;line-height:1.25;margin:20px 0 12px;">'
            'Tu premio te espera</h1>'
            f'<p style="font-size:16px;margin:0 0 24px;">Hola, {escape(name)}.</p>'
            '<table role="presentation" width="100%" cellpadding="0" cellspacing="0">'
            '<tr><td align="center" bgcolor="#f7f3ed" style="padding:28px 16px;'
            'border-radius:16px;"><p style="font-size:12px;letter-spacing:2px;'
            'color:#666666;margin:0 0 12px;">TU PREMIO</p>'
            f'<p style="font-size:30px;font-weight:bold;line-height:1.2;margin:0;">'
            f'{escape(title)}</p></td></tr></table>'
            '<p style="font-size:16px;line-height:1.6;margin:24px 0;">'
            'Muestra tu tarjeta de wallet o este QR al personal del local.</p>'
            f'{wallet_html}'
            '<p style="font-size:16px;font-weight:bold;margin:28px 0 12px;">Tu QR personal</p>'
            '<img src="cid:reward-qr" width="280" height="280" alt="QR de tu cuenta Kivly"'
            ' style="display:block;width:280px;max-width:100%;height:auto;margin:0 auto;">'
            '<p style="font-size:13px;line-height:1.5;color:#666666;margin:12px 0 0;">'
            'Identifica tu cuenta de Kivly. No necesitas instalar una wallet.</p>'
            '</td></tr><tr><td align="center" style="padding:20px 24px;'
            'border-top:1px solid #eeeae5;font-size:12px;line-height:1.6;color:#777777;">'
            'El personal verificar\u00e1 la disponibilidad y las condiciones antes del canje.'
            '<br>Este correo no refleja canjes posteriores.'
            '</td></tr></table></td></tr></table></body></html>',
            subtype="html",
        )
        if logo_png is not None:
            message.get_payload()[1].add_related(
                logo_png, maintype="image", subtype="png", cid="<club-logo>",
                filename="club-logo.png", disposition="inline",
            )
    message.get_payload()[1].add_related(
        image.getvalue(), maintype="image", subtype="png", cid="<reward-qr>",
        filename="kivly-qr.png", disposition="inline",
    )
    return message.as_bytes()


def build_reward_message(sender, recipient, name, prize, *, qr_value=None, wallet_links=()):
    if not is_valid_email(recipient):
        raise ValueError("InvalidRewardEmailContact")
    title = prize.get("name")
    if (not isinstance(title, str) or not title.strip() or len(title) > 200
            or any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in title)):
        raise ValueError("InvalidRewardEmailPrize")
    if not isinstance(name, str) or len(name) > 200:
        name = ""
    links = []
    for label, url in wallet_links:
        parsed = urlsplit(url) if isinstance(url, str) else None
        if (not parsed or parsed.scheme != "https" or not parsed.hostname
                or parsed.username or parsed.password or len(url) > 4096
                or any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in url)):
            raise ValueError("InvalidRewardWalletLink")
        links.append((str(label), url))
    if qr_value is not None and (
        not isinstance(qr_value, str) or not qr_value or len(qr_value) > 4096
        or any(ord(char) < 32 or 127 <= ord(char) <= 159 for char in qr_value)
    ):
        raise ValueError("InvalidRewardQRCode")

    message = EmailMessage(policy=SMTP)
    message["From"] = sender
    message["To"] = recipient
    message["Subject"] = f"Tu premio STAND / Your STAND reward: {title}"
    message.set_content(
        f"Hola, {name}:\n\nTu premio STAND: {title}.\n\n"
        "Muestra el QR o la tarjeta Wallet al personal del evento para recogerlo.\n"
        "Show the QR code or Wallet pass to event staff to collect your reward.\n\n"
        + "".join(f"{label}: {url}\n" for label, url in links)
    )
    link_html = "".join(
        f'<p><a href="{escape(url, quote=True)}">{escape(label)}</a></p>' for label, url in links
    )
    qr_html = ""
    qr_image = None
    if qr_value:
        import qrcode
        from qrcode.image.pure import PyPNGImage

        image = BytesIO()
        qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M, box_size=8, border=4)
        qr.add_data(qr_value, optimize=0)
        qr.make(fit=True)
        qr.make_image(image_factory=PyPNGImage).save(image)
        qr_image = image.getvalue()
        qr_html = (
            '<p style="font-size:16px;font-weight:bold;">Tu QR personal / Your reward QR</p>'
            '<img src="cid:reward-qr" width="240" height="240" alt="QR del premio">'
        )
    message.add_alternative(
        '<!doctype html><html lang="es"><body style="font-family:Arial,Helvetica,sans-serif;'
        'color:#171717;max-width:600px;margin:24px auto;padding:24px;">'
        f'<p>Hola, {escape(name)}.</p><h1>{escape(title)}</h1>'
        '<p>Muestra tu tarjeta Wallet o este QR al personal del evento.</p>'
        '<p>Show your Wallet pass or QR code to event staff.</p>'
        f'{link_html}{qr_html}'
        '<p style="font-size:12px;color:#666;">El email no refleja cambios posteriores en el estado del premio.'
        ' / This email does not reflect later reward status changes.</p></body></html>',
        subtype="html",
    )
    if qr_image is not None:
        message.get_payload()[1].add_related(
            qr_image, maintype="image", subtype="png", cid="<reward-qr>",
            filename="stand-reward-qr.png", disposition="inline",
        )
    return message.as_bytes()
