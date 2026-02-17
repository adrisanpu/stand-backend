"""
Handles OPTIONS preflight for GameApi. No auth so browser preflight succeeds.
Returns 200 with explicit CORS headers so the preflight always has HTTP OK status.
"""

def lambda_handler(event, context):
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "authorization, content-type",
        },
        "body": "",
    }
