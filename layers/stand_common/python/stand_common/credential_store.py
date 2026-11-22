"""Read credentials from Secrets Manager or a named SecureString parameter."""
import json

import boto3


def read_json(reference):
    if reference.startswith("/"):
        response = boto3.client("ssm").get_parameter(Name=reference, WithDecryption=True)
        value = response["Parameter"]["Value"]
    else:
        response = boto3.client("secretsmanager").get_secret_value(SecretId=reference)
        value = response["SecretString"]
    return json.loads(value)
