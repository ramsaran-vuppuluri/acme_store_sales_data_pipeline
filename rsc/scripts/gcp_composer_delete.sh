#!/bin/bash

PROJECT_ID=$DEVSHELL_PROJECT_ID

echo "Current Project ID" $PROJECT_ID

echo "Deleting Cloud Composer Environment acme_sales_data_pipeline"

gcloud composer environments delete acme_sales_data_pipeline

echo "Deleted Cloud Composer Environment acme_sales_data_pipeline"
