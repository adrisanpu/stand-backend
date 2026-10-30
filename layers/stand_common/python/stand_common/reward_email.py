"""Reward notice with an inline, locally generated Kivly customer QR."""
from email.message import EmailMessage
from email.policy import SMTP
from html import escape
from io import BytesIO

from stand_common.kivly import reward_image_url
from stand_common.utils import is_valid_email, is_valid_phone


def build_message(sender, recipient, name, prize, phone):
    if not is_valid_email(recipient) or not is_valid_phone(phone):
        raise ValueError("InvalidRewardEmailContact")
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
    message.get_payload()[1].add_related(
        image.getvalue(), maintype="image", subtype="png", cid="<reward-qr>",
        filename="kivly-qr.png", disposition="inline",
    )
    return message.as_bytes()
