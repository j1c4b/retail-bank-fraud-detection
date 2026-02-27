import json
import time
import random
from datetime import datetime, timedelta
from faker import Faker
from google.cloud import storage, bigquery
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import avro.schema

fake = Faker()

PROJECT_ID = "retail-bank-sim"
BUCKET_NAME = "retail-bank-sim-batch"
DATASET = "fraud_analytics"
TABLE = "transactions"
BATCH_INTERVAL = 300  # 5 minutes
LOCAL_AVRO = "/tmp/transactions_batch.avro"
AVRO_SCHEMA_FILE = "transaction_schema.avsc"
LATE_LOOKBACK_MINUTES = 15  # fraud queries look back extra 15 min

storage_client = storage.Client()
bq_client = bigquery.Client()

# Load Avro schema
with open(AVRO_SCHEMA_FILE, 'r') as f:
    schema = avro.schema.parse(f.read())

FRAUD_ACCOUNTS = [fake.bban() for _ in range(5)]

def generate_transaction():
    is_fraud = random.random() < 0.05
    is_late = random.random() < 0.02  # 2% late

    account = random.choice(FRAUD_ACCOUNTS) if is_fraud else fake.bban()

    if is_late:
        delay_minutes = random.randint(5, 10)
        tx_time = datetime.utcnow() - timedelta(minutes=delay_minutes)
    else:
        tx_time = datetime.utcnow()

    return {
        "transaction_id": fake.uuid4(),
        "account_id": account,
        "card_number": fake.credit_card_number(),
        "masked_card_number": None,  # Will be populated by DLP version
        "amount": float(round(random.uniform(5000, 9999), 2)) if is_fraud
                  else float(round(random.uniform(1, 500), 2)),
        "merchant": fake.company(),
        "location": fake.city(),
        "transaction_timestamp": tx_time.isoformat(),        # EVENT time
        "operation_type": "INSERT",
        "ingestion_timestamp": datetime.utcnow().isoformat()  # ARRIVAL time
    }

def save_to_avro(transactions):
    """Write transactions to Avro file"""
    with open(LOCAL_AVRO, 'wb') as avro_file:
        writer = DataFileWriter(avro_file, DatumWriter(), schema)
        for tx in transactions:
            writer.append(tx)
        writer.close()

def upload_to_gcs(batch_id):
    blob_name = f"batches/batch_{batch_id}.avro"
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(LOCAL_AVRO)
    return f"gs://{BUCKET_NAME}/{blob_name}"

def load_to_bigquery(gcs_uri):
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.AVRO,
        use_avro_logical_types=True,  # Handle timestamps correctly
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # Partition by EVENT time — late data lands in correct partition
        time_partitioning=bigquery.TimePartitioning(
            field="transaction_timestamp"
        ),
        clustering_fields=["account_id"]
    )
    load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()

def run_fraud_query():
    """Look back extra 15 min to catch late arriving transactions"""
    query = f"""
        SELECT
            account_id,
            COUNT(*) as tx_count,
            ROUND(SUM(amount), 2) as total_amount,
            ROUND(MAX(amount), 2) as max_tx,
            COUNTIF(amount > 4999) as fraud_tx,
            MIN(transaction_timestamp) as earliest_event,
            MAX(ingestion_timestamp) as latest_arrival
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
        WHERE transaction_timestamp >= TIMESTAMP_SUB(
            CURRENT_TIMESTAMP(),
            INTERVAL {BATCH_INTERVAL // 60 + LATE_LOOKBACK_MINUTES} MINUTE
        )
        GROUP BY account_id
        HAVING COUNTIF(amount > 4999) > 0
        ORDER BY fraud_tx DESC
        LIMIT 5
    """
    results = bq_client.query(query).result()
    print("\n  🔍 Fraud accounts detected this batch (with late arrival lookback):")
    for row in results:
        print(f"     account:{row.account_id} | "
              f"fraud_tx:{row.fraud_tx} | "
              f"total:${row.total_amount} | "
              f"earliest_event:{str(row.earliest_event)[:19]}")

def run_batch_pipeline(speed=10):
    print(f"📦 Avro Batch Pipeline — flushing every {BATCH_INTERVAL}s")
    print(f"Late arrival lookback: {LATE_LOOKBACK_MINUTES} extra minutes in fraud queries")
    print(f"Format: Apache Avro (column-oriented, schema evolution support)")
    print("=" * 60)

    batch_id = 0
    while True:
        batch_start = time.time()
        transactions = []
        late_count = 0
        fraud_count = 0

        print(f"\nBatch {batch_id} — collecting for {BATCH_INTERVAL} seconds...")

        while time.time() - batch_start < BATCH_INTERVAL:
            tx = generate_transaction()
            transactions.append(tx)

            tx_time = datetime.fromisoformat(tx["transaction_timestamp"])
            arrival_time = datetime.fromisoformat(tx["ingestion_timestamp"])
            if (arrival_time - tx_time).seconds > 120:
                late_count += 1
            if tx["amount"] > 4999:
                fraud_count += 1

            if len(transactions) % 100 == 0:
                print(f"  Collected {len(transactions)} | "
                      f"fraud: {fraud_count} | late: {late_count}")
            time.sleep(1 / speed)

        print(f"\nFlushing batch {batch_id}...")
        save_to_avro(transactions)
        gcs_uri = upload_to_gcs(batch_id)
        print(f"  Uploaded → {gcs_uri} (Avro format)")
        load_to_bigquery(gcs_uri)
        print(f"  Loaded to BigQuery (FREE bulk load + schema evolution) ✓")
        print(f"  Summary: {len(transactions)} total | "
              f"{fraud_count} fraud | {late_count} late arriving")

        run_fraud_query()
        print("=" * 60)
        batch_id += 1

if __name__ == "__main__":
    run_batch_pipeline(speed=10)
