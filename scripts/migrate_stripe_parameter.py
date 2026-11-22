"""Copy a Stripe secret to Parameter Store without printing its value."""
import argparse
import json

import boto3
from botocore.exceptions import BotoCoreError, ClientError


def migrate(session, environment, apply):
    source = f"stand/{environment}/stripe"
    target = f"/{source}"
    value = session.client("secretsmanager").get_secret_value(SecretId=source)["SecretString"]
    if len(value.encode("utf-8")) > 4096:
        raise ValueError("The secret exceeds the 4 KiB standard parameter limit.")
    data = json.loads(value)
    if not isinstance(data, dict) or not (data.get("STRIPE_SECRET_KEY") or data.get("SECRET_KEY")):
        raise ValueError("The Stripe secret does not contain a secret key.")
    if apply:
        session.client("ssm").put_parameter(
            Name=target, Value=value, Type="SecureString", Tier="Standard", Overwrite=False,
        )
    return target


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", choices=("dev", "prod"), required=True)
    parser.add_argument("--profile", default="stand-deploy-cli")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--apply", action="store_true", help="Create the new parameter.")
    args = parser.parse_args(argv)
    try:
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
        target = migrate(session, args.environment, args.apply)
    except (BotoCoreError, ClientError) as exc:
        code = exc.response.get("Error", {}).get("Code") if isinstance(exc, ClientError) else type(exc).__name__
        parser.exit(1, f"AWS request failed: {code}\n")
    except (ValueError, KeyError, TypeError):
        parser.exit(1, "The source secret is missing, invalid, or too large for a standard parameter.\n")
    print(("Created " if args.apply else "Validated; no changes made to ") + target)


if __name__ == "__main__":
    main()
