"""Reward notices with inline, locally generated customer and prize QR images."""
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


def _reward_email_html(
    name, title, *, club_name=None, logo=False, hero_image=None, prize_image=False, instructions=(),
    wallet_links=(), qr_heading=None, qr_alt="QR personal", qr_note=None,
    disclosures=(), include_qr=True,
):
    logo_alt = escape(club_name or "STAND", quote=True)
    prize_image_html = (
        '<img src="cid:prize-photo" width="180" alt=""'
        ' style="display:block;width:180px;max-width:100%;height:auto;margin:0 auto 14px;">'
        if prize_image else ""
    )
    logo_html = (
        f'<img src="cid:club-logo" width="180" alt="{logo_alt}"'
        ' style="display:block;width:180px;max-width:100%;height:auto;margin:0 auto 16px;">'
        if logo else ""
    )
    hero_html = (
        f'<img src="{escape(hero_image, quote=True)}" alt="" width="180"'
        ' style="display:block;width:180px;max-width:100%;height:auto;margin:0 auto 16px;">'
        if hero_image else ""
    )
    club_html = (
        f'<p style="font-size:13px;color:#666666;margin:0;">{escape(club_name)}</p>'
        if club_name else ""
    )
    instructions_html = "".join(
        f'<p style="font-size:14px;line-height:1.6;margin:18px 0 0;">{escape(text)}</p>'
        for text in instructions
    )
    wallet_html = "".join(
        '<table role="presentation" cellpadding="0" cellspacing="0" align="center"'
        ' style="margin:18px auto 0;"><tr><td align="center" bgcolor="#171717"'
        ' style="border-radius:8px;">'
        f'<a href="{escape(url, quote=True)}" style="display:inline-block;padding:13px 20px;'
        f'color:#ffffff;text-decoration:none;font-size:14px;font-weight:bold;">{escape(label)}</a>'
        '</td></tr></table>'
        + (
            f'<p style="font-size:11px;color:#666666;margin:8px 0 0;">{escape(note)}</p>'
            if note else ""
        )
        for label, url, note in wallet_links
    )
    qr_html = (
        f'<p style="font-size:14px;font-weight:bold;margin:24px 0 12px;">{escape(qr_heading)}</p>'
        '<img src="cid:reward-qr" width="280" height="280" alt="'
        f'{escape(qr_alt, quote=True)}" style="display:block;width:280px;max-width:100%;'
        'height:auto;margin:0 auto;">'
        + (
            f'<p style="font-size:12px;line-height:1.5;color:#666666;margin:10px 0 0;">'
            f'{escape(qr_note)}</p>'
            if qr_note else ""
        )
        if include_qr else ""
    )
    footer_html = "<br>".join(escape(text) for text in disclosures)
    return (
        '<!doctype html><html lang="es"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        '</head><body style="margin:0;padding:0;background-color:#f4f1ed;'
        'font-family:Arial,Helvetica,sans-serif;color:#171717;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0"'
        ' bgcolor="#f4f1ed"><tr><td align="center" style="padding:24px 12px;">'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0"'
        ' data-email-layout="reward-card"'
        ' style="max-width:440px;background-color:#ffffff;border:1px solid #e8e3dd;'
        'border-radius:16px;"><tr><td align="center" style="padding:28px 24px;">'
        f'{logo_html}{hero_html}{club_html}'
        '<h1 style="font-size:22px;line-height:1.3;margin:18px 0 8px;">'
        'Tu premio te espera</h1>'
        f'<p style="font-size:14px;margin:0 0 20px;">Hola, {escape(name)}.</p>'
        '<table role="presentation" width="100%" cellpadding="0" cellspacing="0">'
        '<tr><td align="center" bgcolor="#f7f3ed" style="padding:22px 14px;'
        'border-radius:12px;"><p style="font-size:11px;letter-spacing:2px;'
        'color:#666666;margin:0 0 10px;">TU PREMIO</p>'
        f'{prize_image_html}'
        f'<p style="font-size:24px;font-weight:bold;line-height:1.25;margin:0;">'
        f'{escape(title)}</p></td></tr></table>'
        f'{instructions_html}{wallet_html}{qr_html}'
        '</td></tr><tr><td align="center" style="padding:16px 20px;'
        'border-top:1px solid #eeeae5;font-size:11px;line-height:1.6;color:#777777;">'
        f'{footer_html}</td></tr></table></td></tr></table></body></html>'
    )


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
    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M, box_size=10, border=4)
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
    message.add_alternative(
        _reward_email_html(
            name, title, hero_image=photo,
            instructions=(
                "Muestra este QR al personal. No necesitas wallet. El personal comprobara y canjeara el premio en Kivly.",
                "Show this QR to staff. No wallet needed. Staff will check and redeem the reward in Kivly.",
            ),
            qr_heading="Tu QR personal / Scan to redeem",
            qr_alt="QR Kivly",
            disclosures=(
                "Este correo no refleja canjes posteriores.",
                "This email does not reflect subsequent redemptions.",
            ),
        ),
        subtype="html",
    )
    if club_name is not None:
        wallet_text = (
            f"A\u00f1ade tu tarjeta a Apple Wallet o Google Wallet:\n{wallet_url}\n\n"
            if wallet_url else ""
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
            _reward_email_html(
                name, title, club_name=club_name, logo=logo_png is not None,
                instructions=("Muestra tu tarjeta de wallet o este QR al personal del local.",),
                wallet_links=(
                    (("A\u00f1adir tarjeta a mi wallet", wallet_url, "Apple Wallet o Google Wallet"),)
                    if wallet_url else ()
                ),
                qr_heading="Tu QR personal",
                qr_alt="QR de tu cuenta Kivly",
                qr_note="Identifica tu cuenta de Kivly. No necesitas instalar una wallet.",
                disclosures=(
                    "El personal verificar\u00e1 la disponibilidad y las condiciones antes del canje.",
                    "Este correo no refleja canjes posteriores.",
                ),
            ),
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


def build_reward_message(
    sender, recipient, name, prize, *, qr_value=None, wallet_links=(),
    logo_png=None, prize_image_png=None,
):
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
    for image in (logo_png, prize_image_png):
        if image is not None and (
            not isinstance(image, bytes) or len(image) < 33 or len(image) > 5 * 1024 * 1024
            or not image.startswith(b"\x89PNG\r\n\x1a\n") or image[12:16] != b"IHDR"
        ):
            raise ValueError("InvalidRewardEmailImage")
    if sum(len(image) for image in (logo_png, prize_image_png) if image is not None) > 6 * 1024 * 1024:
        raise ValueError("InvalidRewardEmailImage")

    message = EmailMessage(policy=SMTP)
    message["From"] = sender
    message["To"] = recipient
    message["Subject"] = f"Tu premio STAND / Your STAND reward: {title}"
    if qr_value and links:
        instructions = (
            "Muestra el QR o usa el enlace Wallet para recoger tu premio.\n"
            "Show the QR or use the Wallet link to collect your reward."
        )
    elif qr_value:
        instructions = (
            "Muestra este QR al personal del evento para recoger tu premio.\n"
            "Show this QR code to event staff to collect your reward."
        )
    elif links:
        instructions = (
            "Usa el enlace Wallet para acceder a tu premio.\n"
            "Use the Wallet link to access your reward."
        )
    else:
        instructions = ""
    message.set_content(
        f"Hola, {name}:\n\nTu premio STAND: {title}.\n\n"
        f"{instructions}\n\n"
        + "".join(f"{label}: {url}\n" for label, url in links)
    )
    qr_image = None
    if qr_value:
        import qrcode
        from qrcode.image.pure import PyPNGImage

        image = BytesIO()
        qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M, box_size=10, border=4)
        qr.add_data(qr_value, optimize=0)
        qr.make(fit=True)
        qr.make_image(image_factory=PyPNGImage).save(image)
        qr_image = image.getvalue()
    message.add_alternative(
        _reward_email_html(
            name, title, logo=logo_png is not None, prize_image=prize_image_png is not None,
            instructions=tuple(
                line for line in (
                    "Muestra este QR al personal del evento para recoger tu premio."
                    if qr_value and not links else
                    "Muestra el QR o usa el enlace Wallet para recoger tu premio."
                    if qr_value and links else
                    "Usa el enlace Wallet para acceder a tu premio."
                    if links else None,
                    "Show this QR code to event staff to collect your reward."
                    if qr_value and not links else
                    "Show the QR or use the Wallet link to collect your reward."
                    if qr_value and links else
                    "Use the Wallet link to access your reward."
                    if links else None,
                ) if line
            ),
            wallet_links=tuple((label, url, None) for label, url in links),
            qr_heading="Tu QR personal / Your reward QR",
            qr_alt="QR del premio",
            disclosures=(
                "El email no refleja cambios posteriores en el estado del premio.",
                "This email does not reflect later reward status changes.",
            ),
            include_qr=qr_image is not None,
        ),
        subtype="html",
    )
    if qr_image is not None:
        message.get_payload()[1].add_related(
            qr_image, maintype="image", subtype="png", cid="<reward-qr>",
            filename="stand-reward-qr.png", disposition="inline",
        )
    if logo_png is not None:
        message.get_payload()[1].add_related(
            logo_png, maintype="image", subtype="png", cid="<club-logo>",
            filename="stand-logo.png", disposition="inline",
        )
    if prize_image_png is not None:
        message.get_payload()[1].add_related(
            prize_image_png, maintype="image", subtype="png", cid="<prize-photo>",
            filename="stand-prize.png", disposition="inline",
        )
    return message.as_bytes()
