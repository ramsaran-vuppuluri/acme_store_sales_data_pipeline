#!/bin/bash

PROJECT_ID=$DEVSHELL_PROJECT_ID

echo "Current Project ID" $PROJECT_ID

echo "Enabling Cloud Storage Service"

gcloud services enable storage-component.googleapis.com

echo "Enabled Cloud Storage Service"

echo "Enabling Cloud Composer Service"

gcloud services enable composer.googleapis.com

echo "Enabled Cloud Composer Service"

echo "Enabling Cloud Dataproc Service"

gcloud services enable dataproc.googleapis.com

echo "Enabled Cloud Dataproc Service"
