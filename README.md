
# Retail Bank Fraud Detection — BigQuery Migration Simulation

A real-time bank transaction fraud detection pipeline built on GCP.
Simulates a production-grade architecture using Pub/Sub, Dataflow, Cloud DLP, and BigQuery.

---

## Architecture Overview

```
Python Transaction        Pub/Sub           Dataflow            BigQuery
Generator (CDC sim)  →   (buffer)   →   (process + DLP)  →   fraud_analytics
fraud + late data                          mask PII             column security
```

**Key Features:**
- **Fraud simulation**: 5% of transactions are high-value ($5000+) from known fraud accounts
- **Late-arriving data**: 2% of events arrive with 5-10 minute delays (simulates out-of-order CDC events)
- **Event time vs. arrival time**: Tracks both `transaction_timestamp` (when transaction occurred) and `ingestion_timestamp` (when event arrived)

### Full Target Architecture (Phase 3+)

```
Cloud SQL PostgreSQL       Datastream         Pub/Sub           Dataflow           BigQuery
(CDC enabled)         →   (reads logs)  →   (buffer)   →   (process + DLP)  →   fraud_analytics
simulates SQL Server                                           mask PII             column security
```

---

## Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Python | 3.11.x | 3.12+ not supported by Apache Beam |
| gcloud CLI | 557.0.0+ | Google Cloud SDK |
| pip | latest | Upgrade with `pip install --upgrade pip` |

---

## Phase Roadmap

| Phase | What You Build | Status |
|-------|---------------|--------|
| **Phase 1** | GCP setup + Python generates fake transactions → Pub/Sub → BigQuery | ✅ Complete |
| **Phase 2** | Cloud DLP to mask card numbers in-flight before BigQuery | ✅ Complete |
| **Phase 3** | Cloud SQL + Datastream reads real DB CDC logs → Pub/Sub | ⏳ Upcoming |
| **Phase 4** | Dataflow pipeline processes in real time → BigQuery | ⏳ Upcoming |
| **Phase 5** | Column-level security + Fraud detection queries | ⏳ Upcoming |

---

## Phase 1 Setup — Step by Step

### Step 1 — Install gcloud CLI

Download from: https://cloud.google.com/sdk/docs/install

Verify:
```bash
gcloud --version
```

---

### Step 2 — Authenticate

```bash
# Login to Google account
gcloud auth login

# Allow Python scripts to authenticate to GCP
gcloud auth application-default login
```

---

### Step 3 — Create GCP Project

```bash
# Create project
gcloud projects create retail-bank-sim --name="Retail Bank Simulation"

# Set as active project
gcloud config set project retail-bank-sim

# Fix quota project warning
gcloud auth application-default set-quota-project retail-bank-sim
```

Link billing account at:
```
https://console.cloud.google.com/billing/linkedaccount?project=retail-bank-sim
```

---

### Step 4 — Enable APIs

```bash
gcloud services enable \
  pubsub.googleapis.com \
  bigquery.googleapis.com \
  dataflow.googleapis.com \
  dlp.googleapis.com
```

---

### Step 5 — Create Virtual Environment

```bash
# Create project folder
mkdir retail-bank-sim
cd retail-bank-sim

# Create venv with Python 3.11 (required for Apache Beam)
python3.11 -m venv venv

# Activate
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows

# Verify Python version
python --version                # Must show 3.11.x
```

---

### Step 6 — Install Dependencies

```bash
pip install --upgrade pip setuptools wheel

pip install \
  google-cloud-pubsub \
  google-cloud-bigquery \
  google-cloud-dataflow \
  "apache-beam[gcp]" \
  faker \
  python-dotenv
```

> **Note for zsh users:** Square brackets must be quoted — use `"apache-beam[gcp]"`

---

### Step 7 — Create BigQuery Dataset and Table

```bash
# Create dataset
bq mk --dataset \
  --location=US \
  --description="Fraud analytics simulation" \
  retail-bank-sim:fraud_analytics

# Create transactions table
bq mk --table \
  fraud_analytics.transactions \
  transaction_id:STRING,\
account_id:STRING,\
card_number:STRING,\
amount:FLOAT,\
merchant:STRING,\
location:STRING,\
transaction_timestamp:TIMESTAMP,\
operation_type:STRING,\
ingestion_timestamp:TIMESTAMP
```

---

### Step 8 — Create Pub/Sub Topic and Subscription

```bash
# Create topic
gcloud pubsub topics create transactions-topic

# Create subscription
gcloud pubsub subscriptions create transactions-sub \
  --topic=transactions-topic \
  --ack-deadline=60
```

---

### Step 9 — Create Python Scripts

#### `transaction_generator.py` — Simulates CDC events with fraud and late-arriving data

```python
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

# Simulate fraudulent accounts (5 fixed accounts that behave suspiciously)
FRAUD_ACCOUNTS = [fake.bban() for _ in range(5)]

def generate_transaction():
    is_fraud = random.random() < 0.05  # 5% fraud rate
    is_late = random.random() < 0.02   # 2% late arriving (simulates out-of-order events)

    account = random.choice(FRAUD_ACCOUNTS) if is_fraud else fake.bban()

    # Late-arriving events: transaction time is 5-10 minutes in the past
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
        "transaction_timestamp": tx_time.isoformat(),           # EVENT time
        "ingestion_timestamp": datetime.utcnow().isoformat(),   # ARRIVAL time
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
```

#### `bq_consumer.py` — Reads from Pub/Sub, detects late events, writes to BigQuery

```python
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
```

---

### Step 10 — Run the Pipeline

Open two terminals, both with venv activated:

```bash
# Terminal 1 — Generate transactions
python transaction_generator.py

# Terminal 2 — Consume and write to BigQuery
python bq_consumer.py
```

---

### Step 11 — Verify Data in BigQuery

```sql
-- See latest transactions
SELECT * FROM `retail-bank-sim.fraud_analytics.transactions`
ORDER BY ingestion_timestamp DESC
LIMIT 20;

-- Find suspicious high-value transactions (potential fraud)
SELECT * FROM `retail-bank-sim.fraud_analytics.transactions`
WHERE amount > 4999
ORDER BY transaction_timestamp DESC;

-- Account velocity check — >3 transactions = suspicious
SELECT
  account_id,
  COUNT(*) as tx_count,
  SUM(amount) as total_amount
FROM `retail-bank-sim.fraud_analytics.transactions`
GROUP BY account_id
HAVING COUNT(*) > 3
ORDER BY tx_count DESC;

-- Detect late-arriving events (event time vs. arrival time)
SELECT
  transaction_id,
  amount,
  merchant,
  transaction_timestamp,
  ingestion_timestamp,
  TIMESTAMP_DIFF(
    TIMESTAMP(ingestion_timestamp),
    TIMESTAMP(transaction_timestamp),
    MINUTE
  ) as delay_minutes
FROM `retail-bank-sim.fraud_analytics.transactions`
WHERE TIMESTAMP_DIFF(
  TIMESTAMP(ingestion_timestamp),
  TIMESTAMP(transaction_timestamp),
  MINUTE
) > 2
ORDER BY delay_minutes DESC;
```

---

## Understanding Late-Arriving Data

**What is late-arriving data?**
In real-world streaming systems, events don't always arrive in chronological order. A transaction that occurred at 10:00 AM might arrive at 10:08 AM due to network delays, database replication lag, or CDC processing delays.

**Why does this matter?**
- **Event Time**: When the transaction actually occurred (`transaction_timestamp`)
- **Arrival Time**: When we received the event (`ingestion_timestamp`)
- **The gap**: Real-world systems must handle this time skew correctly

**How this simulation works:**
- 2% of generated transactions are backdated by 5-10 minutes
- The consumer detects events where arrival time > event time + 2 minutes
- BigQuery partitions data by event time, not arrival time (correct approach)

**Real-world example:**
```
Event Time: 2025-01-15 10:00:00  (when customer swiped card)
Arrival Time: 2025-01-15 10:08:00  (when CDC event reached Pub/Sub)
Delay: 8 minutes ⚠️  LATE
```

This prepares you for real streaming challenges like watermarking and windowing in Dataflow (Phase 3).

---

## Phase 2 — Cloud DLP to Mask Card Numbers

Now we'll add PCI-DSS compliance by masking credit card numbers **before** they're stored in BigQuery.

### Step 1 — Enable Cloud DLP API

```bash
gcloud services enable dlp.googleapis.com
```

---

### Step 2 — Install DLP Python Library

```bash
pip install google-cloud-dlp
```

---

### Step 3 — Update BigQuery Schema

Add a `masked_card_number` field to store the DLP-masked card number:

```bash
bq update retail-bank-sim:fraud_analytics.transactions /dev/stdin <<'EOF'
[
  {
    "name": "transaction_id",
    "type": "STRING"
  },
  {
    "name": "account_id",
    "type": "STRING"
  },
  {
    "name": "card_number",
    "type": "STRING"
  },
  {
    "name": "masked_card_number",
    "type": "STRING"
  },
  {
    "name": "amount",
    "type": "FLOAT"
  },
  {
    "name": "merchant",
    "type": "STRING"
  },
  {
    "name": "location",
    "type": "STRING"
  },
  {
    "name": "transaction_timestamp",
    "type": "TIMESTAMP"
  },
  {
    "name": "operation_type",
    "type": "STRING"
  },
  {
    "name": "ingestion_timestamp",
    "type": "TIMESTAMP"
  }
]
EOF
```

---

### Step 4 — Test DLP Masking

Before running the full pipeline, verify DLP works:

```bash
python test_dlp.py
```

Expected output:
```
Testing Cloud DLP Card Number Masking
============================================================
Original: 5425233430109903
Masked:   5425************

Original: 378282246310005
Masked:   378************

✓ DLP masking is working correctly!
```

---

### Step 5 — Run DLP-Enabled Pipeline

Use the new DLP-enabled consumer instead of the basic one:

```bash
# Terminal 1 — Generate transactions (same as before)
python transaction_generator.py

# Terminal 2 — DLP-enabled consumer
python bq_consumer_dlp.py
```

**What's different:**
- Card numbers are automatically masked using Cloud DLP before BigQuery insert
- Console output shows masked cards: `card:5425************`
- Both `card_number` (original) and `masked_card_number` (DLP-masked) are stored
- In Phase 5, we'll add column-level security to restrict access to original card numbers

---

### Step 6 — Verify Masked Data in BigQuery

```sql
-- See masked vs. original card numbers
SELECT
  transaction_id,
  card_number,
  masked_card_number,
  amount,
  merchant
FROM `retail-bank-sim.fraud_analytics.transactions`
ORDER BY ingestion_timestamp DESC
LIMIT 10;

-- Fraud analysts should only see masked cards
SELECT
  transaction_id,
  masked_card_number,  -- Safe for analysts to see
  amount,
  merchant,
  location
FROM `retail-bank-sim.fraud_analytics.transactions`
WHERE amount > 4999
ORDER BY transaction_timestamp DESC;
```

---

## Phase 3 Preview — Real CDC with Cloud SQL + Datastream

- Provision Cloud SQL (PostgreSQL) instance
- Enable CDC (logical replication) on the database
- Configure Datastream connection profile pointing to Cloud SQL
- Datastream reads WAL logs and publishes to Pub/Sub automatically
- Python generator is no longer needed

---

## Phase 4 Preview — Dataflow Pipeline

Replace simple `bq_consumer_dlp.py` with a full Apache Beam pipeline:
- Exactly-once delivery via BigQuery Storage Write API
- Deduplication on `transaction_id`
- Schema validation
- Dead letter queue for malformed records
- Integrate DLP masking directly into Dataflow transforms

---

## Phase 5 Preview — Column-Level Security

- Apply BigQuery column-level security policies
- Create fraud analyst role that can only query `masked_card_number` (not `card_number`)
- Admin role can access original card numbers for compliance investigations
- All access logged via Cloud Audit Logs for PCI-DSS compliance

---

## GCP Services Used

| Service | Purpose | Phase |
|---------|---------|-------|
| Cloud Pub/Sub | Message buffer for streaming ingestion | 1 (streaming) |
| Cloud Storage (GCS) | Batch file staging before BigQuery load | Alternative (batch) |
| BigQuery | Analytical data warehouse for fraud analytics | 1 |
| Cloud DLP | PII tokenization and masking in-flight | 2 |
| Cloud SQL (PostgreSQL) | Source transaction database with CDC | 3 |
| Datastream | CDC replication from Cloud SQL to Pub/Sub | 3 |
| Dataflow (Apache Beam) | Stream processing pipeline with DLP integration | 4 |
| BigQuery Column Security | Restrict access to sensitive PII columns | 5 |
| Cloud Audit Logs | PCI-DSS compliance — who accessed what data | 5 |

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `zsh: no matches found: apache-beam[gcp]` | Use quotes: `"apache-beam[gcp]"` |
| `ModuleNotFoundError: No module named 'pkg_resources'` | Run `pip install --upgrade pip setuptools wheel` |
| Apache Beam build fails | Must use Python 3.11 — not 3.12 or 3.13 |
| Quota project warning | Run `gcloud auth application-default set-quota-project retail-bank-sim` |
| APIs not enabled | Ensure billing is linked before enabling APIs |

---

## Project Structure

```
retail-bank-sim/
├── venv/                        # Python 3.11 virtual environment
├── transaction_generator.py     # Simulates CDC events → Pub/Sub (fraud + late data)
├── bq_consumer.py               # Phase 1: Basic Pub/Sub → BigQuery writer
├── bq_consumer_dlp.py           # Phase 2: DLP-enabled Pub/Sub consumer with card masking
├── batch_generator.py           # Alternative: Basic batch processing via GCS (cost-optimized)
├── batch_generator_dlp.py       # Alternative: DLP-enabled batch processing with card masking
├── test_dlp.py                  # Test script to verify Cloud DLP masking
├── pipeline.py                  # (Phase 4) Dataflow/Beam pipeline with DLP
└── README.md                    # This file
```

---

## Alternative: Batch Processing (Cost-Optimized)

For production workloads where cost matters more than real-time latency, use batch processing:

### Batch Setup

```bash
# Create GCS bucket for batch files
gsutil mb -l US gs://retail-bank-sim-batch

# Enable Cloud Storage API
gcloud services enable storage.googleapis.com
```

### Run Batch Pipeline

**Without DLP (basic):**
```bash
python batch_generator.py
```

**With DLP (PCI-DSS compliant):**
```bash
python batch_generator_dlp.py
```

**How it works:**
- Collects transactions for 5 minutes (configurable)
- **DLP version**: Masks each card number via Cloud DLP API before CSV write
- Writes batch to CSV file
- Uploads to Cloud Storage
- BigQuery batch load (FREE - no streaming insert costs)
- Runs fraud detection query with 15-minute lookback for late arrivals
- Event-time partitioning ensures late data lands in correct partitions

**DLP Performance Note:**
- DLP API calls add ~50-100ms per transaction
- For 10 tx/sec over 5 min: ~3,000 DLP calls per batch
- Recommended: Use `batch_generator_dlp.py` for production PCI-DSS compliance
- For performance testing: Use `batch_generator.py` (no DLP overhead)

**Cost savings:**
- Streaming inserts: $0.01 per 200 MB
- Batch loads: **FREE**
- For 1M transactions/day: ~$150/month → **$0/month**

**Streaming vs. Batch Comparison:**

| Feature | Streaming (Pub/Sub) | Batch (GCS) |
|---------|-------------------|-------------|
| Latency | ~1 second | 5+ minutes |
| Cost | $0.01 per 200 MB | FREE |
| Use Case | Real-time fraud alerts | Cost-optimized analytics |
| Late Data Handling | Immediate | Query lookback window |
| Best For | Critical transactions | Historical analysis |

**DLP Masking Options:**

| Script | DLP Support | When to Use |
|--------|------------|-------------|
| `bq_consumer.py` | ❌ No | Phase 1 learning only |
| `bq_consumer_dlp.py` | ✅ Yes | Production streaming (PCI-DSS) |
| `batch_generator.py` | ❌ No | Phase 1 learning only |
| `batch_generator_dlp.py` | ✅ Yes | Production batch (PCI-DSS) |

---

## Pipeline Options Summary

| Approach | Script | DLP | Latency | Cost | Use Case |
|----------|--------|-----|---------|------|----------|
| **Streaming** | `transaction_generator.py` + `bq_consumer.py` | ❌ | ~1s | Paid | Phase 1 learning |
| **Streaming + DLP** | `transaction_generator.py` + `bq_consumer_dlp.py` | ✅ | ~1s | Paid | Production real-time |
| **Batch** | `batch_generator.py` | ❌ | 5min | FREE | Phase 1 learning |
| **Batch + DLP** | `batch_generator_dlp.py` | ✅ | 5min | FREE | Production cost-optimized |

---

## Quick Start Guide

**Phase 1 — Basic Streaming Pipeline:**
```bash
# Terminal 1
python transaction_generator.py

# Terminal 2
python bq_consumer.py
```

**Phase 2 — DLP Masking (Streaming):**
```bash
# First, test DLP
python test_dlp.py

# Terminal 1
python transaction_generator.py

# Terminal 2 — Use DLP-enabled consumer
python bq_consumer_dlp.py
```

**Alternative — Batch Processing (Cost-Optimized):**
```bash
# Single terminal - generates batches every 5 minutes

# Basic (no DLP)
python batch_generator.py

# With DLP masking
python batch_generator_dlp.py
```

**View Results:**
```bash
# Open BigQuery Console
open "https://console.cloud.google.com/bigquery?project=retail-bank-sim"

# Or query via CLI
bq query --use_legacy_sql=false '
SELECT transaction_id, masked_card_number, amount, merchant
FROM `retail-bank-sim.fraud_analytics.transactions`
WHERE amount > 4999
ORDER BY transaction_timestamp DESC
LIMIT 10'
```

---

*Built for Retail Bank Database Engineering interview preparation.*
*Simulates a production CDC pipeline for real-time fraud analytics on GCP.*
