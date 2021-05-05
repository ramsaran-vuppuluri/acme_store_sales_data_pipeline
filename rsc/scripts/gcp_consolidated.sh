#!/bin/bash

PROJECT_ID=$DEVSHELL_PROJECT_ID
BUCKET_NAME=$DEVSHELL_PROJECT_ID
REGION=northamerica-northeast1
ZONE=northamerica-northeast1-b
ACME_CLOUD_COMPOSER_PIPELINE=acme-pipeline
DATAPROC_CLUSTER_NAME=acme-sales-cluster
DATAPROC_MASTER_MACHINE_TYPE=n1-standard-2
IMAGE_VERSION=1.5-debian10

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

echo "Creating bucket" $BUCKET_NAME

gsutil mb -c standard -l northamerica-northeast1 gs://$BUCKET_NAME

echo "Created bucket" $BUCKET_NAME

echo "Creating Cloud Composer Environment "$ACME_CLOUD_COMPOSER_PIPELINE

gcloud composer environments create $ACME_CLOUD_COMPOSER_PIPELINE \
--location=$REGION \
--zone=$ZONE \
--python-version=3

echo "Created Cloud Composer Environment "$ACME_CLOUD_COMPOSER_PIPELINE

echo "Adding Airflow variable gcp_project_id as " $PROJECT_ID
gcloud composer environments run $ACME_CLOUD_COMPOSER_PIPELINE --location=$REGION variables -- --set gcp_project_id $PROJECT_ID
echo "Added Airflow variable gcp_project_id as " $PROJECT_ID

echo "Adding Airflow variable gcp_region as " $REGION
gcloud composer environments run $ACME_CLOUD_COMPOSER_PIPELINE --location=$REGION variables -- --set gcp_region $REGION
echo "Added Airflow variable gcp_region as " $REGION

echo "Adding Airflow variable gcp_zone as "$ZONE
gcloud composer environments run $ACME_CLOUD_COMPOSER_PIPELINE --location=$REGION variables -- --set gcp_zone $ZONE
echo "Added Airflow variable gcp_zone as "$ZONE

echo "Adding Airflow variable gcs_bucket_name as " $PROJECT_ID
gcloud composer environments run $ACME_CLOUD_COMPOSER_PIPELINE --location=$REGION variables -- --set gcs_bucket_name $PROJECT_ID
echo "Added Airflow variable gcs_bucket_name as " $PROJECT_ID

echo "Adding Airflow variable image_version as "$IMAGE_VERSION
gcloud composer environments run $ACME_CLOUD_COMPOSER_PIPELINE --location=$REGION variables -- --set image_version $IMAGE_VERSION
echo "Added Airflow variable image_version as "$IMAGE_VERSION

echo "Adding Airflow variable dataproc_cluster_name as "$DATAPROC_CLUSTER_NAME
gcloud composer environments run $ACME_CLOUD_COMPOSER_PIPELINE --location=$REGION variables -- --set dataproc_cluster_name $DATAPROC_CLUSTER_NAME
echo "Added Airflow variable dataproc_cluster_name as "$DATAPROC_CLUSTER_NAME

echo "Adding Airflow variable dataproc_master_machine_type as "$DATAPROC_MASTER_MACHINE_TYPE
gcloud composer environments run $ACME_CLOUD_COMPOSER_PIPELINE --location=$REGION variables -- --set dataproc_master_machine_type $DATAPROC_MASTER_MACHINE_TYPE
echo "Added Airflow variable dataproc_master_machine_type as "$DATAPROC_MASTER_MACHINE_TYPE
