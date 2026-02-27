import json
from datetime import datetime, timedelta
from google.cloud import pubsub_v1, bigquery

PROJECT_ID = "retail-bank-sim"
SUBSCRIPTION_ID = "transactions-sub"
TABLE_ID = "retail-bank-sim.fraud_analytics.transactions"

subscriber = pubsub_v1.SubscriberClient()
bq_client = bigquery.Client()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

LATE_THRESHOLD_MINUTES = 2  # flag if tx_time is older than 2 min

def is_late_arriving(tx):
    tx_time = datetime.fromisoformat(tx["transaction_timestamp"])
    arrival_time = datetime.fromisoformat(tx["ingestion_timestamp"])
    delta = arrival_time - tx_time
    return delta > timedelta(minutes=LATE_THRESHOLD_MINUTES), delta

def callback(message):
    tx = json.loads(message.data.decode("utf-8"))

    late, delta = is_late_arriving(tx)

    # Insert into correct partition based on event time (transaction_timestamp)
    errors = bq_client.insert_rows_json(TABLE_ID, [tx])

    if errors:
        print(f"❌ BQ Error: {errors}")
    else:
        if late:
            minutes = int(delta.seconds / 60)
            print(f"⚠️  LATE +{minutes}min | ${tx['amount']} | "
                  f"event:{tx['transaction_timestamp'][:19]} | "
                  f"arrived:{tx['ingestion_timestamp'][:19]}")
        elif tx["amount"] > 4999:
            print(f"🚨 FRAUD inserted | ${tx['amount']} | {tx['merchant']}")
        else:
            print(f"✓ ${tx['amount']} | {tx['merchant']} | {tx['location']}")

    message.ack()

streaming_pull = subscriber.subscribe(subscription_path, callback=callback)
print("Listening for transactions...")
print("Legend: ✓ normal | ⚠️  late arriving | 🚨 fraud")
print("=" * 60)

try:
    streaming_pull.result()
except KeyboardInterrupt:
    streaming_pull.cancel()
    print("\nStopped.")
