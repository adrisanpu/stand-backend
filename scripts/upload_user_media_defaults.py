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
    parser.add_argument(
        "--download-from-base-url",
        default=DEFAULT_BASE_URL,
        help="If local files are missing, download the default logo images from this base URL.",
    )
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=args.region)

    with tempfile.TemporaryDirectory(prefix="stand-default-media-") as tmp_dir:
        tmp = Path(tmp_dir)
        logo_white = Path(args.logo_white_path) if args.logo_white_path else tmp / "logo_stand_letters_white.png"
        logo_black = Path(args.logo_black_path) if args.logo_black_path else tmp / "logo_stand_letters_black.png"
        base = args.download_from_base_url.rstrip("/")
        if not logo_white.exists():
            _download(f"{base}/images/logo_stand_letters_white.png", logo_white)
        if not logo_black.exists():
            _download(f"{base}/images/logo_stand_letters_black.png", logo_black)

        _upload_file(s3, args.bucket, "defaults/logo/logo_stand_letters_white.png", logo_white, "image/png")
        _upload_file(s3, args.bucket, "defaults/logo/logo_stand_letters_black.png", logo_black, "image/png")

        print("Uploaded defaults successfully:")
        print(f"  s3://{args.bucket}/defaults/logo/logo_stand_letters_white.png")
        print(f"  s3://{args.bucket}/defaults/logo/logo_stand_letters_black.png")


if __name__ == "__main__":
    main()
