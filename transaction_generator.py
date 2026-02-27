import json
import time
import random
from datetime import datetime, timedelta
from faker import Faker
from google.cloud import pubsub_v1

fake = Faker()

PROJECT_ID = "retail-bank-sim"
TOPIC_ID = "transactions-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

FRAUD_ACCOUNTS = [fake.bban() for _ in range(5)]

def generate_transaction():
    is_fraud = random.random() < 0.05
    is_late = random.random() < 0.02  # 2% late arriving

    account = random.choice(FRAUD_ACCOUNTS) if is_fraud else fake.bban()

    if is_late:
        delay_minutes = random.randint(5, 10)
        tx_time = datetime.utcnow() - timedelta(minutes=delay_minutes)
        late_flag = True
    else:
        tx_time = datetime.utcnow()
        late_flag = False

    return {
        "transaction_id": fake.uuid4(),
        "account_id": account,
        "card_number": fake.credit_card_number(),
        "amount": round(random.uniform(5000, 9999), 2) if is_fraud
                  else round(random.uniform(1, 500), 2),
        "merchant": fake.company(),
        "location": fake.city(),
        "transaction_timestamp": tx_time.isoformat(),      # EVENT time
        "ingestion_timestamp": datetime.utcnow().isoformat(),  # ARRIVAL time
        "operation_type": "INSERT",
        "is_late": late_flag
    }

def publish_transactions(speed=10):
    print("Publishing transactions to Pub/Sub...")
    print("Legend: ✓ normal | ⚠️  late arriving | 🚨 fraud")
    print("=" * 60)

    count = 0
    late_count = 0
    fraud_count = 0

    while True:
        tx = generate_transaction()

        # Remove is_late before publishing (not in BQ schema)
        is_late = tx.pop("is_late")
        is_fraud = tx["amount"] > 4999

        data = json.dumps(tx).encode("utf-8")
        future = publisher.publish(topic_path, data)
        future.result()

        count += 1
        if is_late:
            late_count += 1
            delay = (datetime.utcnow() - datetime.fromisoformat(tx["transaction_timestamp"])).seconds // 60
            print(f"⚠️  LATE ({delay}min delay) | ${tx['amount']} | {tx['merchant']} | {tx['location']}")
        elif is_fraud:
            fraud_count += 1
            print(f"🚨 FRAUD | ${tx['amount']} | {tx['merchant']} | {tx['location']}")
        
        if count % 50 == 0:
            print(f"--- Stats: {count} total | {fraud_count} fraud | {late_count} late ---")

        time.sleep(1 / speed)

if __name__ == "__main__":
    publish_transactions(speed=10)
