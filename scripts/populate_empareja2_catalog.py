"""
Populate stand-prod-catalog-table with empareja2 character items (correct structure).

Reads stand-backend/scripts/empareja2_catalog_items.json and writes each item
to the catalog table. Each item has: catalogId, pairId (unique), pairGroupId,
characterId (number), characterName.

If you already have old empareja2 rows (e.g. PAIR#0001... or PAIR#0001#CHAR#0001...),
run scripts/migrate_empareja2_remove_pair_rows.py first to remove PAIR rows.
Items with the same (catalogId, pairId) are overwritten; to do a full replace,
delete all EMPAREJA2#CHARACTERS#v1 rows in the table before running this script.

Usage:
  From stand-backend with AWS credentials configured:
    python scripts/populate_empareja2_catalog.py

  Optional env:
    CATALOG_TABLE  default: stand-prod-catalog-table
"""
import json
import os
from pathlib import Path

import boto3

CATALOG_TABLE = os.environ.get("CATALOG_TABLE", "stand-prod-catalog-table")
SCRIPT_DIR = Path(__file__).resolve().parent
ITEMS_JSON = SCRIPT_DIR / "empareja2_catalog_items.json"


def main():
    with open(ITEMS_JSON, encoding="utf-8") as f:
        data = json.load(f)

    catalog_id = data["catalogId"]
    items = data["items"]

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(CATALOG_TABLE)

    with table.batch_writer() as batch:
        for it in items:
            row = {
                "catalogId": catalog_id,
                "pairId": it["pairId"],
                "pairGroupId": it["pairGroupId"],
                "characterId": int(it["characterId"]),
                "characterName": it["characterName"],
            }
            batch.put_item(Item=row)
            print(f"  {row['pairId']}  {row['characterName']}  (pairGroupId={row['pairGroupId']})")

    print(f"Done. Wrote {len(items)} items to {CATALOG_TABLE}.")


if __name__ == "__main__":
    main()
