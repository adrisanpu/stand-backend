import json
import boto3

def lambda_handler(event, context):
    TABLE = "stand-prod-catalog-table"
    dynamodb = boto3.resource("dynamodb")
    t = dynamodb.Table(TABLE)

    items = [
        {
            "catalogId": "SEMAFORO#QUIZ#v1",
            "itemId": "Q#0001",
            "questionId": "q1",
            "orderIndex": 1,
            "text": "🚦 ¿En qué color estás ahora mismo?",
            "options": [
            { "title": "🔴 Rojo", "answerId": "a" },
            { "title": "🟡 Amarillo", "answerId": "b" },
            { "title": "🟢 Verde", "answerId": "c" }
            ]
        },
        {
            "catalogId": "SEMAFORO#QUIZ#v1",
            "itemId": "Q#0002",
            "questionId": "q2",
            "orderIndex": 2,
            "text": "🍽️ ¿Qué prefieres?",
            "options": [
            { "title": "🍕 Pizza", "answerId": "a" },
            { "title": "🍣 Sushi", "answerId": "b" },
            { "title": "🍔 Burger", "answerId": "c" }
            ]
        },
        {
            "catalogId": "SEMAFORO#QUIZ#v1",
            "itemId": "Q#0003",
            "questionId": "q3",
            "orderIndex": 3,
            "text": "🌍 ¿Dónde estarías ahora mismo?",
            "options": [
            { "title": "🌊 Mar", "answerId": "a" },
            { "title": "⛰️ Montaña", "answerId": "b" },
            { "title": "🏙️ Ciudad", "answerId": "c" }
            ]
        },
        {
            "catalogId": "SEMAFORO#QUIZ#v1",
            "itemId": "Q#0004",
            "questionId": "q4",
            "orderIndex": 4,
            "text": "🎉 Plan perfecto:",
            "options": [
            { "title": "🍻 Salir con gente", "answerId": "a" },
            { "title": "🏠 Plan tranquilo", "answerId": "b" },
            { "title": "✨ Improvisar", "answerId": "c" }
            ]
        },
        {
            "catalogId": "SEMAFORO#QUIZ#v1",
            "itemId": "Q#0005",
            "questionId": "q5",
            "orderIndex": 5,
            "text": "🥤 ¿Qué te representa más?",
            "options": [
            { "title": "☕ Café", "answerId": "a" },
            { "title": "🍺 Cerveza", "answerId": "b" },
            { "title": "💧 Agua", "answerId": "c" }
            ]
        }
        ]

    with t.batch_writer() as batch:
        for it in items:
            batch.put_item(Item=it)

    print("OK")