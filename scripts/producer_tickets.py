import os
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from faker import Faker


fake = Faker("fr_FR")

BOOTSTRAP_SERVERS = os.getenv(
    "REDPANDA_BOOTSTRAP_SERVERS",
    "localhost:19092",
)

producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
    }
)

TOPIC = "client_tickets"


request_types = [
    "facturation",
    "incident_technique",
    "demande_information",
    "modification_compte",
    "resiliation",
]

priorities = [
    "basse",
    "moyenne",
    "haute",
    "critique",
]


def generate_ticket():
    return {
        "ticket_id": str(uuid.uuid4()),
        "client_id": random.randint(1000, 9999),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "request": fake.sentence(),
        "request_type": random.choice(request_types),
        "priority": random.choice(priorities),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"Erreur lors de l'envoi : {err}")
    else:
        print(
            f"Ticket envoyé dans {msg.topic()} "
            f"[partition {msg.partition()}] "
            f"offset {msg.offset()}"
        )


try:
    while True:
        ticket = generate_ticket()

        producer.produce(
            topic=TOPIC,
            key=ticket["ticket_id"],
            value=json.dumps(ticket),
            callback=delivery_report,
        )

        producer.poll(0)

        print(ticket)

        time.sleep(2)

except KeyboardInterrupt:
    print("\nArrêt du producteur...")

finally:
    producer.flush()