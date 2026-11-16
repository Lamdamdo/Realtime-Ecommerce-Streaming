
import os
import json
import uuid
import random

from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

# Load credentials
load_dotenv()

# Kafka configuration
config = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET")
}

topic = os.getenv("KAFKA_TOPIC")

# Create Kafka producer
producer = Producer(config)


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered to {msg.topic()} | "
            f"Partition: {msg.partition()} | "
            f"Offset: {msg.offset()}"
        )


# Generate and publish 10 order events
for i in range(30):

    order = {
        "event_id": str(uuid.uuid4()),
        "order_id":f"ORD{uuid.uuid4().hex[:10].upper()}",
        "customer_id": f"CUST-{random.randint(1, 50)}",
        "product_id": f"PROD-{random.randint(1, 20)}",
        "quantity": random.randint(1, 5),
        "amount": round(random.uniform(100, 5000), 2),
        "status": random.choices(
    	["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"],
   	weights=[40, 25, 25, 10],
    	k=1
	)[0],
        "payment_method": random.choice(
            ["UPI", "CARD", "COD", "NET_BANKING"]
        ),
        "event_timestamp": datetime.now(
            timezone.utc
        ).isoformat()
    }
    print(
        order["order_id"],
        order["status"],
        order["payment_method"]
    )
    producer.produce(
        topic=topic,
        key=order["order_id"],
        value=json.dumps(order),
        callback=delivery_report
    )

    producer.poll(0)

# Wait for delivery confirmation
remaining = producer.flush(30)

if remaining > 0:
    print(f"{remaining} messages were not delivered")
else:
    print("All messages processed")
