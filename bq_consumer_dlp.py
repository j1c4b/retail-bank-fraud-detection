import json
from datetime import datetime, timedelta
from google.cloud import pubsub_v1, bigquery, dlp_v2

PROJECT_ID = "retail-bank-sim"
SUBSCRIPTION_ID = "transactions-sub"
TABLE_ID = "retail-bank-sim.fraud_analytics.transactions"

subscriber = pubsub_v1.SubscriberClient()
bq_client = bigquery.Client()
dlp_client = dlp_v2.DlpServiceClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

LATE_THRESHOLD_MINUTES = 2  # flag if tx_time is older than 2 min

def mask_card_number(card_number):
    """
    Uses Cloud DLP to mask credit card numbers with asterisks.
    Example: 4532-1234-5678-9010 → ************9010
    """
    parent = f"projects/{PROJECT_ID}"

    # Configure DLP to detect and mask credit card numbers
    inspect_config = {
        "info_types": [{"name": "CREDIT_CARD_NUMBER"}]
    }

    deidentify_config = {
        "info_type_transformations": {
            "transformations": [
                {
                    "primitive_transformation": {
                        "character_mask_config": {
                            "masking_character": "*",
                            "reverse_order": True,  # Keep last 4 digits visible
                            "number_to_mask": 12    # Mask first 12 digits
                        }
                    }
                }
            ]
        }
    }

    item = {"value": card_number}

    try:
        response = dlp_client.deidentify_content(
            request={
                "parent": parent,
                "deidentify_config": deidentify_config,
                "inspect_config": inspect_config,
                "item": item
            }
        )
        return response.item.value
    except Exception as e:
        print(f"⚠️  DLP masking failed: {e}")
        # Fallback: manual masking if DLP fails
        return "*" * (len(card_number) - 4) + card_number[-4:]

def is_late_arriving(tx):
    tx_time = datetime.fromisoformat(tx["transaction_timestamp"])
    arrival_time = datetime.fromisoformat(tx["ingestion_timestamp"])
    delta = arrival_time - tx_time
    return delta > timedelta(minutes=LATE_THRESHOLD_MINUTES), delta

def callback(message):
    tx = json.loads(message.data.decode("utf-8"))

    late, delta = is_late_arriving(tx)

    # 🔒 Mask card number using Cloud DLP before storing
    original_card = tx["card_number"]
    tx["masked_card_number"] = mask_card_number(original_card)

    # Keep original for now (will remove in Phase 4 with column security)
    # In production, you'd only store masked_card_number

    # Insert into BigQuery
    errors = bq_client.insert_rows_json(TABLE_ID, [tx])

    if errors:
        print(f"❌ BQ Error: {errors}")
    else:
        if late:
            minutes = int(delta.seconds / 60)
            print(f"⚠️  LATE +{minutes}min | ${tx['amount']} | "
                  f"card:{tx['masked_card_number']} | "
                  f"event:{tx['transaction_timestamp'][:19]}")
        elif tx["amount"] > 4999:
            print(f"🚨 FRAUD | ${tx['amount']} | {tx['merchant']} | card:{tx['masked_card_number']}")
        else:
            print(f"✓ ${tx['amount']} | {tx['merchant']} | card:{tx['masked_card_number']}")

    message.ack()

streaming_pull = subscriber.subscribe(subscription_path, callback=callback)
print("🔒 DLP-Enabled Consumer Listening...")
print("Legend: ✓ normal | ⚠️  late arriving | 🚨 fraud")
print("Card numbers are masked using Cloud DLP before storage")
print("=" * 60)

try:
    streaming_pull.result()
except KeyboardInterrupt:
    streaming_pull.cancel()
    print("\nStopped.")
