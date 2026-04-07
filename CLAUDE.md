# stand-backend

## WHY
Python 3.13 AWS SAM serverless backend. Stack name: `stand-backend-prod`, region `us-east-1`.
All business logic lives in Lambda functions; shared utilities in the stand_common layer.

## WHAT
**Commands:** `sam build` | `sam deploy --config-env prod` | `sam local invoke <Fn> -e events/test.json` | `pytest tests/`

**DynamoDB Tables (4):**
- `stand-prod-game-table` — PK: gameId; GSI: gsi-ownerUserId, gsi-gameName
- `stand-prod-gameplayer-table` — PK: gameId, SK: playerId(N); GSI: gsi-instagramPSID, gsi-raffleEligible
- `stand-prod-user-table` — PK: userId; fields: plan, activeUntil
- `stand-prod-catalog-table` — PK: catalogId, SK: pairId

**Layer utils** (`layers/stand_common/python/stand_common/utils.py`):
`log`, `_resp`, `_iso_now`, `_parse_iso`, `_get_claims`, `_read_json_body`, `_as_int`, `_json_sanitize`, `get_game_type_blob`, `set_game_type_blob`

**Game types (UPPERCASE in DB):** EMPAREJA2, T1MER, RULET4, L3TRAS, SEMAFORO, INFOCARDS, CUPIDO

**Plans:** `free` (1 game, 25 players) | `event`/`enterprise` (unlimited, all features)
Plan check: verify BOTH `plan` value AND `_parse_iso(activeUntil) > now`

## HOW
```python
# Lambda handler pattern
from stand_common.utils import log, _resp, _get_claims, _read_json_body, _json_sanitize

def lambda_handler(event, context):
    claims = _get_claims(event)           # raises 401 if missing
    user_id = claims["sub"]
    body = _read_json_body(event)         # raises 400 if invalid JSON
    # ... business logic ...
    return _resp(200, _json_sanitize(data))  # always sanitize before _resp
```
- Never serialize DynamoDB Decimals directly — always use `_as_int()` or `_json_sanitize()`
- `set_game_type_blob` does two update_item calls — do NOT collapse into one
- CORS: cors-fn handles OPTIONS without auth — never add auth to OPTIONS routes
- IAM roles: StandProdGameRole | StandProdBillingRole | StandProdWebhookRole | StandProdSenderRole | StandProdInfocardsRole
