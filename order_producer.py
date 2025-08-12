# kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
# kafka-topics.sh --create --topic order_status --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1


from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

CUSTOMERS = [101, 102, 103, 104, 105]
PRODUCTS = ["Laptop", "Headphones", "Monitor", "Mouse", "Keyboard"]

def generate_order(order_id):
    return {
        "order_id": order_id,
        "customer_id": random.choice(CUSTOMERS),
        "product": random.choice(PRODUCTS),
        "quantity": random.randint(1, 3),
        "price": round(random.uniform(50, 500), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    order_id = 1
    while True:
        burst = random.choice([True, False, False])  # simulate bursts sometimes
        for _ in range(random.randint(1, 5) if burst else 1):
            order = generate_order(order_id)
            producer.send("orders", value=order)
            print(f"[Order Producer] Sent: {order}")
            order_id += 1

        time.sleep(random.uniform(0.5, 2))  # random delay between sends
