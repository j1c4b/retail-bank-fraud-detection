# Security Guidelines

## What NOT to commit

❌ **NEVER commit these files:**
- Service account JSON files (`*.json`)
- API keys or secrets
- `.env` files with actual credentials
- GCP authentication tokens

## Safe to commit

✅ **Safe to include:**
- `.env.example` (template with placeholder values)
- Python source code
- README and documentation
- `.gitignore`

## Setup Instructions

1. **Create your own GCP project** - Don't use the project ID shown in the code examples
2. **Set environment variables** (optional):
   ```bash
   cp .env.example .env
   # Edit .env with your actual GCP project details
   ```
3. **Authenticate via gcloud**:
   ```bash
   gcloud auth login
   gcloud auth application-default login
   gcloud config set project YOUR-PROJECT-ID
   ```

## Authentication

This project uses **Application Default Credentials (ADC)**:
- No service account keys needed in code
- Authenticates via `gcloud auth application-default login`
- Never stores credentials in the repository

## Hardcoded Project IDs

The example code contains `PROJECT_ID = "retail-bank-sim"` for demonstration purposes. 

**To use your own project:**
- Replace `"retail-bank-sim"` with your actual project ID in each script, OR
- Use environment variables (see `.env.example`)

This is safe because:
- GCP project IDs are not secrets
- Access requires separate authentication
- No billing or resources can be accessed without proper IAM permissions
