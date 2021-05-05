#!/bin/bash

PROJECT_ID=$DEVSHELL_PROJECT_ID
BUCKET_NAME=$DEVSHELL_PROJECT_ID

echo "Current Project ID" $PROJECT_ID

echo "Creating bucket" $BUCKET_NAME

gsutil mb -c standard -l northamerica-northeast1 gs://$BUCKET_NAME

echo "Created bucket" $BUCKET_NAME
