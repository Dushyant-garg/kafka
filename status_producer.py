from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

STATUSES = ["placed", "payment_processed", "shipped", "delivered", "cancelled"]

def generate_status(order_id, status):
    return {
        "order_id": order_id,
        "status": status,
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    # We will keep track of orders and send statuses for them randomly
    active_orders = list(range(1, 51))  # simulate first 50 orders
    while True:
        order_id = random.choice(active_orders)

        # Randomly decide if we send delayed or out-of-order events
        if random.random() < 0.2:
            # Delayed event: timestamp in past
            status = generate_status(order_id, random.choice(STATUSES))
            status["timestamp"] = (datetime.utcnow().timestamp() - random.randint(60, 300))  # 1-5 min delay
            status["timestamp"] = datetime.utcfromtimestamp(status["timestamp"]).isoformat()
        elif random.random() < 0.2:
            # Out-of-order: send later stage before earlier one
            status = generate_status(order_id, random.choice(STATUSES[2:]))  # skip first stages
        else:
            # Normal order progression
            status = generate_status(order_id, random.choice(STATUSES))

        producer.send("order_status", value=status)
        print(f"[Status Producer] Sent: {status}")

        # Burst of updates
        if random.random() < 0.1:
            for _ in range(random.randint(2, 5)):
                order_id = random.choice(active_orders)
                status = generate_status(order_id, random.choice(STATUSES))
                producer.send("order_status", value=status)
                print(f"[Status Producer] Burst Sent: {status}")

        time.sleep(random.uniform(0.3, 1.5))
