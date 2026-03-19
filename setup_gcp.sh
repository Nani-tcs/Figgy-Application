#!/bin/bash

# Configuration variables - REPLACE WITH YOUR VALUES
PROJECT_ID="trainocat-1773726781563"
REGION="us-central1" # e.g., us-central1
SERVICE_ACCOUNT_NAME="figgy-service-account"
SA_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# --- Project Setup ---
echo "Configuring gcloud project and region..."
gcloud config set project "$PROJECT_ID"
gcloud config set run/region "$REGION"
gcloud config set functions/region "$REGION"

# --- Enable necessary APIs ---
echo "Enabling required GCP APIs..."
gcloud services enable \
  run.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  pubsub.googleapis.com \
  firestore.googleapis.com \
  cloudtasks.googleapis.com \
  apigateway.googleapis.com \
  iam.googleapis.com \
  servicecontrol.googleapis.com \
  servicemanagement.googleapis.com \
  cloudresourcemanager.googleapis.com # Needed for policy bindings

# --- Service Account Setup ---
echo "Creating service account: ${SERVICE_ACCOUNT_NAME}..."
gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
  --display-name="Figgy Food Delivery Service Account" || true # '|| true' to ignore if already exists

echo "Assigning IAM roles to service account: ${SA_EMAIL}..."
# Common roles for all services
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/datastore.user" --quiet
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/pubsub.publisher" --quiet

# Roles specific to Pub/Sub Push subscribers (Cloud Run services)
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/pubsub.subscriber" --quiet

# Role for Cloud Tasks to enqueue
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/cloudtasks.enqueuer" --quiet

# Role for Cloud Tasks to invoke HTTP Cloud Function (OIDC token generation)
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/iam.serviceAccountUser" --quiet

# Role for Cloud Run services to invoke other services (e.g. Delivery Orchestrator) if using authenticated calls
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/run.invoker" --quiet


# --- GCP Resource Creation ---
echo "Creating Firestore database..."
gcloud firestore databases create --location="$REGION" || true # '|| true' to ignore if already exists

echo "Creating Pub/Sub topics..."
gcloud pubsub topics create orders.place || true
gcloud pubsub topics create orders.created || true
gcloud pubsub topics create orders.accepted || true
gcloud pubsub topics create orders.rejected || true

echo "Creating Cloud Tasks queue: delivery-simulation-queue..."
gcloud tasks queues create delivery-simulation-queue --location="$REGION" || true

echo "GCP Setup Complete."
echo "Remember to update [YOUR_PROJECT_ID] and [YOUR_REGION] in this script before running."
echo "Also, ensure the 'Cloud Functions Developer' role is granted to the Google-managed service account for Cloud Build (service-[PROJECT_NUMBER]@cloudbuild.gserviceaccount.com) for deploying Cloud Functions via Cloud Build."