#!/bin/bash
set -e

# CONFIGURATION
# Try to get project ID from gcloud, fallback to user input if needed
PROJECT_ID=$(gcloud config get-value project)
if [ -z "$PROJECT_ID" ]; then
    echo "Error: No default project set in gcloud. Please run 'gcloud config set project <PROJECT_ID>' first."
    exit 1
fi

GITHUB_REPO="Vadoid/iceberg_explorer"
SERVICE_ACCOUNT_NAME="github-actions-sa"
POOL_NAME="github-pool"
PROVIDER_NAME="github-provider"

echo "Setting up WIF for project: $PROJECT_ID"
echo "Repo: $GITHUB_REPO"

# 1. Enable APIs
echo "Enabling necessary APIs..."
gcloud services enable iamcredentials.googleapis.com \
    cloudresourcemanager.googleapis.com \
    iam.googleapis.com

# 2. Create Service Account
echo "Creating Service Account..."
if ! gcloud iam service-accounts describe "${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" &>/dev/null; then
    gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
        --display-name="GitHub Actions Service Account"
    echo "Created Service Account: $SERVICE_ACCOUNT_NAME"
else
    echo "Service Account $SERVICE_ACCOUNT_NAME already exists"
fi

SA_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# 3. Grant Permissions
echo "Granting permissions..."
# App Engine Admin
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/appengine.appAdmin" --condition=None

# Storage Admin (for tests and deployment)
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.admin" --condition=None

# Service Account User (to act as itself)
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/iam.serviceAccountUser" --condition=None

# Compute Storage Admin (for building images)
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/compute.storageAdmin" --condition=None

# 4. Create Workload Identity Pool
echo "Creating Workload Identity Pool..."
if ! gcloud iam workload-identity-pools describe "$POOL_NAME" --location="global" &>/dev/null; then
    gcloud iam workload-identity-pools create "$POOL_NAME" \
        --location="global" \
        --display-name="GitHub Actions Pool"
    echo "Created Pool: $POOL_NAME"
else
    echo "Pool $POOL_NAME already exists"
fi

POOL_ID=$(gcloud iam workload-identity-pools describe "$POOL_NAME" --location="global" --format="value(name)")

# 5. Create Provider
echo "Creating Workload Identity Provider..."
if ! gcloud iam workload-identity-pools providers describe "$PROVIDER_NAME" --location="global" --workload-identity-pool="$POOL_NAME" &>/dev/null; then
    gcloud iam workload-identity-pools providers create-oidc "$PROVIDER_NAME" \
        --location="global" \
        --workload-identity-pool="$POOL_NAME" \
        --display-name="GitHub Actions Provider" \
        --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository" \
        --attribute-condition="assertion.repository=='$GITHUB_REPO'" \
        --issuer-uri="https://token.actions.githubusercontent.com"
    echo "Created Provider: $PROVIDER_NAME"
else
    echo "Provider $PROVIDER_NAME already exists"
fi

# 6. Allow GitHub Repo to impersonate Service Account
echo "Binding GitHub repo to Service Account..."
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
    --role="roles/iam.workloadIdentityUser" \
    --member="principalSet://iam.googleapis.com/${POOL_ID}/attribute.repository/${GITHUB_REPO}" --condition=None

PROVIDER_NAME_FULL=$(gcloud iam workload-identity-pools providers describe "$PROVIDER_NAME" --location="global" --workload-identity-pool="$POOL_NAME" --format="value(name)")

echo ""
echo "✅ Setup Complete!"
echo ""
echo "=== SECRETS FOR GITHUB ==="
echo "GCP_PROJECT_ID: $PROJECT_ID"
echo "WIF_PROVIDER: $PROVIDER_NAME_FULL"
echo "WIF_SERVICE_ACCOUNT: $SA_EMAIL"
echo "=========================="
