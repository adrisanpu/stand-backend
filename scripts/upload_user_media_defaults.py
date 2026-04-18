import argparse
import os
import tempfile
import urllib.request
from pathlib import Path

import boto3


DEFAULT_BASE_URL = "https://playstand.app"


def _download(url: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url) as response:
        data = response.read()
    target.write_bytes(data)


def _upload_file(s3_client, bucket: str, key: str, file_path: Path, content_type: str) -> None:
    s3_client.upload_file(
        str(file_path),
        bucket,
        key,
        ExtraArgs={"ContentType": content_type},
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Temporary uploader for Stand default media in S3.")
    parser.add_argument("--bucket", required=True, help="S3 bucket name (e.g. stand-prod-user-media-<account>)")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    parser.add_argument(
        "--logo-white-path",
        help="Local path to white letters png (oscuro / dark UI). Default: download from base URL.",
    )
    parser.add_argument(
        "--logo-black-path",
        help="Local path to black letters png (claro / light UI). Default: download from base URL.",
    )
    parser.add_argument("--idle-path", help="Local path to stand idle avatar mp4")
    parser.add_argument("--win-path", help="Local path to stand win avatar mp4")
    parser.add_argument("--lose-path", help="Local path to stand lose avatar mp4")
    parser.add_argument(
        "--download-from-base-url",
        default=DEFAULT_BASE_URL,
        help="If local files are missing, download from this base URL (/images/logo_stand_letters_*.png and /avatar/avatar-*.mp4).",
    )
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=args.region)

    with tempfile.TemporaryDirectory(prefix="stand-default-media-") as tmp_dir:
        tmp = Path(tmp_dir)
        logo_white = Path(args.logo_white_path) if args.logo_white_path else tmp / "logo_stand_letters_white.png"
        logo_black = Path(args.logo_black_path) if args.logo_black_path else tmp / "logo_stand_letters_black.png"
        idle_path = Path(args.idle_path) if args.idle_path else tmp / "stand-idle.mp4"
        win_path = Path(args.win_path) if args.win_path else tmp / "stand-win.mp4"
        lose_path = Path(args.lose_path) if args.lose_path else tmp / "stand-lose.mp4"

        base = args.download_from_base_url.rstrip("/")
        if not logo_white.exists():
            _download(f"{base}/images/logo_stand_letters_white.png", logo_white)
        if not logo_black.exists():
            _download(f"{base}/images/logo_stand_letters_black.png", logo_black)
        if not idle_path.exists():
            _download(f"{base}/avatar/avatar-idle.mp4", idle_path)
        if not win_path.exists():
            _download(f"{base}/avatar/avatar-win.mp4", win_path)
        if not lose_path.exists():
            _download(f"{base}/avatar/avatar-lose.mp4", lose_path)

        _upload_file(s3, args.bucket, "defaults/logo/logo_stand_letters_white.png", logo_white, "image/png")
        _upload_file(s3, args.bucket, "defaults/logo/logo_stand_letters_black.png", logo_black, "image/png")
        _upload_file(s3, args.bucket, "defaults/avatar/stand-idle.mp4", idle_path, "video/mp4")
        _upload_file(s3, args.bucket, "defaults/avatar/stand-win.mp4", win_path, "video/mp4")
        _upload_file(s3, args.bucket, "defaults/avatar/stand-lose.mp4", lose_path, "video/mp4")

        print("Uploaded defaults successfully:")
        print(f"  s3://{args.bucket}/defaults/logo/logo_stand_letters_white.png")
        print(f"  s3://{args.bucket}/defaults/logo/logo_stand_letters_black.png")
        print(f"  s3://{args.bucket}/defaults/avatar/stand-idle.mp4")
        print(f"  s3://{args.bucket}/defaults/avatar/stand-win.mp4")
        print(f"  s3://{args.bucket}/defaults/avatar/stand-lose.mp4")


if __name__ == "__main__":
    main()
