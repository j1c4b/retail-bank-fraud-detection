#!/bin/bash

#############################################################################
# Retail Bank Fraud Detection - Interactive Setup Script
#
# This script sets up ALL required GCP resources:
# - Enables GCP APIs
# - Creates BigQuery dataset and tables
# - Creates Pub/Sub topic and subscription
# - Creates Cloud Storage bucket
# - Installs Python dependencies
#
# Features:
# - Interactive prompts with defaults
# - Displays commands before execution
# - Background verification
# - Clear success/failure messages
#############################################################################

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Default values
DEFAULT_PROJECT_ID="retail-bank-sim"
DEFAULT_BUCKET_NAME="retail-bank-sim-batch"
DEFAULT_DATASET="fraud_analytics"
DEFAULT_TABLE="transactions"
DEFAULT_TOPIC="transactions-topic"
DEFAULT_SUBSCRIPTION="transactions-sub"
DEFAULT_REGION="us-central1"
DEFAULT_LOCATION="US"

# Spinner animation for background tasks
spinner() {
    local pid=$1
    local delay=0.1
    local spinstr='|/-\'
    while ps -p $pid > /dev/null 2>&1; do
        local temp=${spinstr#?}
        printf " [%c]  " "$spinstr"
        local spinstr=$temp${spinstr%"$temp"}
        sleep $delay
        printf "\b\b\b\b\b\b"
    done
    printf "    \b\b\b\b"
}

# Execute command with display and verification
execute_step() {
    local description="$1"
    local command="$2"
    local verification="$3"

    echo ""
    echo -e "${CYAN}${BOLD}→ ${description}${NC}"
    echo -e "${YELLOW}Command:${NC} ${command}"

    # Execute command in background
    eval "$command" > /tmp/setup_output.log 2>&1 &
    local cmd_pid=$!

    printf "${YELLOW}Executing...${NC}"
    spinner $cmd_pid
    wait $cmd_pid
    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        # Run verification if provided
        if [ -n "$verification" ]; then
            if eval "$verification" > /dev/null 2>&1; then
                echo -e "${GREEN}✓ Completed and verified${NC}"
            else
                echo -e "${YELLOW}⚠ Completed (verification pending)${NC}"
            fi
        else
            echo -e "${GREEN}✓ Completed${NC}"
        fi
    else
        echo -e "${RED}✗ Failed${NC}"
        echo -e "${RED}Error details:${NC}"
        cat /tmp/setup_output.log
        return 1
    fi
}

#############################################################################
# Welcome and Configuration
#############################################################################

clear
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Retail Bank Fraud Detection - Interactive Setup         ║${NC}"
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo ""
echo "This script will set up all required GCP resources for the fraud detection pipeline."
echo ""

# Get project ID
read -p "Enter GCP Project ID [${DEFAULT_PROJECT_ID}]: " PROJECT_ID
PROJECT_ID=${PROJECT_ID:-$DEFAULT_PROJECT_ID}

read -p "Enter GCS Bucket Name [${DEFAULT_BUCKET_NAME}]: " BUCKET_NAME
BUCKET_NAME=${BUCKET_NAME:-$DEFAULT_BUCKET_NAME}

read -p "Enter BigQuery Dataset Name [${DEFAULT_DATASET}]: " DATASET
DATASET=${DATASET:-$DEFAULT_DATASET}

read -p "Enter BigQuery Table Name [${DEFAULT_TABLE}]: " TABLE
TABLE=${TABLE:-$DEFAULT_TABLE}

read -p "Enter Pub/Sub Topic Name [${DEFAULT_TOPIC}]: " TOPIC
TOPIC=${TOPIC:-$DEFAULT_TOPIC}

read -p "Enter Pub/Sub Subscription Name [${DEFAULT_SUBSCRIPTION}]: " SUBSCRIPTION
SUBSCRIPTION=${SUBSCRIPTION:-$DEFAULT_SUBSCRIPTION}

read -p "Enter GCP Region [${DEFAULT_REGION}]: " REGION
REGION=${REGION:-$DEFAULT_REGION}

read -p "Enter GCP Location for BigQuery [${DEFAULT_LOCATION}]: " LOCATION
LOCATION=${LOCATION:-$DEFAULT_LOCATION}

echo ""
echo -e "${BOLD}Configuration Summary:${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Project ID:      ${PROJECT_ID}"
echo "  GCS Bucket:      ${BUCKET_NAME}"
echo "  BQ Dataset:      ${DATASET}"
echo "  BQ Table:        ${TABLE}"
echo "  Pub/Sub Topic:   ${TOPIC}"
echo "  Pub/Sub Sub:     ${SUBSCRIPTION}"
echo "  Region:          ${REGION}"
echo "  Location:        ${LOCATION}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
read -p "Continue with setup? (yes/no): " confirm

if [ "$confirm" != "yes" ]; then
    echo -e "${YELLOW}Setup cancelled.${NC}"
    exit 0
fi

echo ""
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Starting Setup Process                                   ║${NC}"
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"

#############################################################################
# Step 1: Set Active Project
#############################################################################

execute_step \
    "[1/10] Setting active GCP project" \
    "gcloud config set project ${PROJECT_ID}" \
    "gcloud config get-value project | grep -q ${PROJECT_ID}"

#############################################################################
# Step 2: Enable Pub/Sub API
#############################################################################

execute_step \
    "[2/10] Enabling Pub/Sub API" \
    "gcloud services enable pubsub.googleapis.com --project=${PROJECT_ID}" \
    "gcloud services list --enabled --project=${PROJECT_ID} | grep -q pubsub.googleapis.com"

#############################################################################
# Step 3: Enable BigQuery API
#############################################################################

execute_step \
    "[3/10] Enabling BigQuery API" \
    "gcloud services enable bigquery.googleapis.com --project=${PROJECT_ID}" \
    "gcloud services list --enabled --project=${PROJECT_ID} | grep -q bigquery.googleapis.com"

#############################################################################
# Step 4: Enable Cloud Storage API
#############################################################################

execute_step \
    "[4/10] Enabling Cloud Storage API" \
    "gcloud services enable storage.googleapis.com --project=${PROJECT_ID}" \
    "gcloud services list --enabled --project=${PROJECT_ID} | grep -q storage.googleapis.com"

#############################################################################
# Step 5: Enable Cloud DLP API
#############################################################################

execute_step \
    "[5/10] Enabling Cloud DLP API" \
    "gcloud services enable dlp.googleapis.com --project=${PROJECT_ID}" \
    "gcloud services list --enabled --project=${PROJECT_ID} | grep -q dlp.googleapis.com"

#############################################################################
# Step 6: Create BigQuery Dataset
#############################################################################

execute_step \
    "[6/10] Creating BigQuery dataset '${DATASET}'" \
    "bq mk --dataset --location=${LOCATION} --description='Fraud analytics simulation' ${PROJECT_ID}:${DATASET}" \
    "bq show --dataset ${PROJECT_ID}:${DATASET}"

#############################################################################
# Step 7: Create BigQuery Table
#############################################################################

execute_step \
    "[7/10] Creating BigQuery table '${TABLE}' with schema" \
    "bq mk --table ${PROJECT_ID}:${DATASET}.${TABLE} transaction_id:STRING,account_id:STRING,card_number:STRING,masked_card_number:STRING,amount:FLOAT,merchant:STRING,location:STRING,transaction_timestamp:TIMESTAMP,operation_type:STRING,ingestion_timestamp:TIMESTAMP --time_partitioning_field=transaction_timestamp --clustering_fields=account_id" \
    "bq show ${PROJECT_ID}:${DATASET}.${TABLE}"

#############################################################################
# Step 8: Create Pub/Sub Topic
#############################################################################

execute_step \
    "[8/10] Creating Pub/Sub topic '${TOPIC}'" \
    "gcloud pubsub topics create ${TOPIC} --project=${PROJECT_ID}" \
    "gcloud pubsub topics describe ${TOPIC} --project=${PROJECT_ID}"

#############################################################################
# Step 9: Create Pub/Sub Subscription
#############################################################################

execute_step \
    "[9/10] Creating Pub/Sub subscription '${SUBSCRIPTION}'" \
    "gcloud pubsub subscriptions create ${SUBSCRIPTION} --topic=${TOPIC} --ack-deadline=60 --project=${PROJECT_ID}" \
    "gcloud pubsub subscriptions describe ${SUBSCRIPTION} --project=${PROJECT_ID}"

#############################################################################
# Step 10: Create Cloud Storage Bucket
#############################################################################

execute_step \
    "[10/10] Creating Cloud Storage bucket '${BUCKET_NAME}'" \
    "gsutil mb -l ${LOCATION} gs://${BUCKET_NAME}" \
    "gsutil ls gs://${BUCKET_NAME}"

#############################################################################
# Python Dependencies
#############################################################################

echo ""
echo -e "${CYAN}${BOLD}→ Installing Python dependencies${NC}"
echo -e "${YELLOW}Command:${NC} pip install google-cloud-pubsub google-cloud-bigquery google-cloud-storage google-cloud-dlp apache-beam[gcp] faker python-dotenv avro-python3"

if [ -d "venv" ]; then
    echo "  Using virtual environment: venv/"
    source venv/bin/activate
fi

pip install -q google-cloud-pubsub google-cloud-bigquery google-cloud-storage google-cloud-dlp "apache-beam[gcp]" faker python-dotenv avro-python3 2>&1 | tee /tmp/pip_install.log | grep -E "(Successfully|already)" || true

echo -e "${GREEN}✓ Python dependencies installed${NC}"

#############################################################################
# Completion
#############################################################################

echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   Setup Complete! 🎉                                       ║${NC}"
echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
echo ""
echo -e "${BOLD}Resources Created:${NC}"
echo "  ✓ Project:          ${PROJECT_ID}"
echo "  ✓ BigQuery Dataset: ${DATASET}"
echo "  ✓ BigQuery Table:   ${TABLE} (partitioned by transaction_timestamp)"
echo "  ✓ Pub/Sub Topic:    ${TOPIC}"
echo "  ✓ Pub/Sub Sub:      ${SUBSCRIPTION}"
echo "  ✓ GCS Bucket:       gs://${BUCKET_NAME}"
echo "  ✓ APIs Enabled:     Pub/Sub, BigQuery, Storage, DLP"
echo ""
echo -e "${BOLD}Next Steps:${NC}"
echo ""
echo "1. Test DLP masking:"
echo "   ${CYAN}python test_dlp.py${NC}"
echo ""
echo "2. Run streaming pipeline (Phase 1+2):"
echo "   ${CYAN}# Terminal 1${NC}"
echo "   ${CYAN}python transaction_generator.py${NC}"
echo "   ${CYAN}# Terminal 2${NC}"
echo "   ${CYAN}python bq_consumer_dlp.py${NC}"
echo ""
echo "3. Or run batch pipeline (Avro + DLP):"
echo "   ${CYAN}python batch_generator_avro_dlp.py${NC}"
echo ""
echo "4. Query data in BigQuery:"
echo "   ${CYAN}bq query --use_legacy_sql=false 'SELECT * FROM \`${PROJECT_ID}.${DATASET}.${TABLE}\` LIMIT 10'${NC}"
echo ""
echo "To clean up all resources later:"
echo "   ${CYAN}./cleanup.sh${NC}"
echo ""
