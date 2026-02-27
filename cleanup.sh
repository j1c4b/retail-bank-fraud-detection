#!/bin/bash

#############################################################################
# Retail Bank Fraud Detection - Complete Cleanup Script
#
# This script removes ALL resources created for this project:
# - BigQuery dataset and tables
# - Pub/Sub topics and subscriptions
# - Cloud Storage buckets
# - Disables GCP services (optional)
#
# WARNING: This will DELETE all data! Make sure you have backups.
#############################################################################

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

PROJECT_ID="retail-bank-sim"
BUCKET_NAME="retail-bank-sim-batch"
DATASET="fraud_analytics"
TABLE="transactions"
TOPIC="transactions-topic"
SUBSCRIPTION="transactions-sub"

echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Retail Bank Fraud Detection - Cleanup Script            ║${NC}"
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo ""
echo -e "${YELLOW}⚠️  WARNING: This will DELETE all project resources!${NC}"
echo ""
echo "Project ID: ${PROJECT_ID}"
echo "Resources to be deleted:"
echo "  - BigQuery dataset: ${DATASET}"
echo "  - Pub/Sub topic: ${TOPIC}"
echo "  - Pub/Sub subscription: ${SUBSCRIPTION}"
echo "  - GCS bucket: ${BUCKET_NAME}"
echo ""
read -p "Are you sure you want to continue? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo -e "${YELLOW}Cleanup cancelled.${NC}"
    exit 0
fi

echo ""
echo -e "${BLUE}Starting cleanup process...${NC}"
echo ""

#############################################################################
# 1. Delete Pub/Sub Subscription
#############################################################################

echo -e "${YELLOW}[1/6] Deleting Pub/Sub subscription...${NC}"
echo "Command: gcloud pubsub subscriptions delete ${SUBSCRIPTION} --quiet"

if gcloud pubsub subscriptions describe ${SUBSCRIPTION} --project=${PROJECT_ID} &>/dev/null; then
    gcloud pubsub subscriptions delete ${SUBSCRIPTION} --project=${PROJECT_ID} --quiet 2>/dev/null
    echo -e "${GREEN}✓ Subscription '${SUBSCRIPTION}' deleted${NC}"
else
    echo -e "${YELLOW}⊘ Subscription '${SUBSCRIPTION}' not found (skipping)${NC}"
fi
echo ""

#############################################################################
# 2. Delete Pub/Sub Topic
#############################################################################

echo -e "${YELLOW}[2/6] Deleting Pub/Sub topic...${NC}"
echo "Command: gcloud pubsub topics delete ${TOPIC} --quiet"

if gcloud pubsub topics describe ${TOPIC} --project=${PROJECT_ID} &>/dev/null; then
    gcloud pubsub topics delete ${TOPIC} --project=${PROJECT_ID} --quiet 2>/dev/null
    echo -e "${GREEN}✓ Topic '${TOPIC}' deleted${NC}"
else
    echo -e "${YELLOW}⊘ Topic '${TOPIC}' not found (skipping)${NC}"
fi
echo ""

#############################################################################
# 3. Delete BigQuery Dataset (includes all tables)
#############################################################################

echo -e "${YELLOW}[3/6] Deleting BigQuery dataset and all tables...${NC}"
echo "Command: bq rm -r -f -d ${PROJECT_ID}:${DATASET}"

if bq show --dataset ${PROJECT_ID}:${DATASET} &>/dev/null; then
    bq rm -r -f -d ${PROJECT_ID}:${DATASET} 2>/dev/null
    echo -e "${GREEN}✓ Dataset '${DATASET}' and all tables deleted${NC}"
else
    echo -e "${YELLOW}⊘ Dataset '${DATASET}' not found (skipping)${NC}"
fi
echo ""

#############################################################################
# 4. Delete Cloud Storage Bucket
#############################################################################

echo -e "${YELLOW}[4/6] Deleting Cloud Storage bucket...${NC}"
echo "Command: gsutil -m rm -r gs://${BUCKET_NAME}"

if gsutil ls gs://${BUCKET_NAME} &>/dev/null; then
    # Delete all objects first, then bucket
    echo "  Deleting all objects in bucket..."
    gsutil -m rm -r gs://${BUCKET_NAME}/** 2>/dev/null || true
    gsutil rb gs://${BUCKET_NAME} 2>/dev/null
    echo -e "${GREEN}✓ Bucket 'gs://${BUCKET_NAME}' deleted${NC}"
else
    echo -e "${YELLOW}⊘ Bucket 'gs://${BUCKET_NAME}' not found (skipping)${NC}"
fi
echo ""

#############################################################################
# 5. Disable GCP APIs (optional)
#############################################################################

echo -e "${YELLOW}[5/6] Disabling GCP APIs (optional)...${NC}"
read -p "Do you want to disable GCP APIs? (yes/no): " disable_apis

if [ "$disable_apis" == "yes" ]; then
    echo "Command: gcloud services disable [pubsub, bigquery, storage, dlp]"

    echo "  Disabling Pub/Sub API..."
    gcloud services disable pubsub.googleapis.com --project=${PROJECT_ID} --quiet 2>/dev/null || true

    echo "  Disabling Cloud Storage API..."
    gcloud services disable storage.googleapis.com --project=${PROJECT_ID} --quiet 2>/dev/null || true

    echo "  Disabling Cloud DLP API..."
    gcloud services disable dlp.googleapis.com --project=${PROJECT_ID} --quiet 2>/dev/null || true

    echo "  Disabling BigQuery API..."
    gcloud services disable bigquery.googleapis.com --project=${PROJECT_ID} --quiet 2>/dev/null || true

    echo -e "${GREEN}✓ APIs disabled${NC}"
else
    echo -e "${YELLOW}⊘ Skipping API disable${NC}"
fi
echo ""

#############################################################################
# 6. Clean local files
#############################################################################

echo -e "${YELLOW}[6/6] Cleaning local temporary files...${NC}"

if [ -f /tmp/transactions_batch.csv ]; then
    rm /tmp/transactions_batch.csv
    echo -e "${GREEN}✓ Deleted /tmp/transactions_batch.csv${NC}"
fi

if [ -f /tmp/transactions_batch.avro ]; then
    rm /tmp/transactions_batch.avro
    echo -e "${GREEN}✓ Deleted /tmp/transactions_batch.avro${NC}"
fi

echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   Cleanup Complete!                                        ║${NC}"
echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
echo ""
echo "All resources have been cleaned up."
echo ""
echo "To completely remove the project:"
echo "  gcloud projects delete ${PROJECT_ID}"
echo ""
echo "To recreate the environment, run:"
echo "  ./setup.sh"
echo ""
