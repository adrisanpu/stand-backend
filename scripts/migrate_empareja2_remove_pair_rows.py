"""
One-off migration: remove EMPAREJA2 PAIR rows from stand-prod-catalog-table.

PAIR rows (pairId PAIR#0001 ... PAIR#0005) are redundant; the assigner only uses
CHARACTER rows. CHARACTER rows must have pairGroupId or pairNumericId (same value
for both characters in a pair). This script deletes only the 5 PAIR rows.

Usage:
  Set CATALOG_TABLE (default: stand-prod-catalog-table) and run with AWS credentials
  for the target account:
    python scripts/migrate_empareja2_remove_pair_rows.py

Requires: boto3
"""
import os
import boto3

CATALOG_TABLE = os.environ.get("CATALOG_TABLE", "stand-prod-catalog-table")
EMPAREJA2_CATALOG_ID = "EMPAREJA2#CHARACTERS#v1"
PAIR_ROW_IDS = ["PAIR#0001", "PAIR#0002", "PAIR#0003", "PAIR#0004", "PAIR#0005"]


def main():
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(CATALOG_TABLE)
    for pair_id in PAIR_ROW_IDS:
        table.delete_item(
            Key={
                "catalogId": EMPAREJA2_CATALOG_ID,
                "pairId": pair_id,
            }
        )
        print(f"Deleted {EMPAREJA2_CATALOG_ID} / {pair_id}")
    print("Done. CHARACTER rows must have pairGroupId or pairNumericId for assigner to find partners.")


if __name__ == "__main__":
    main()
