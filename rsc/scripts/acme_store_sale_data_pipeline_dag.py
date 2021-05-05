import datetime

from airflow import models

from airflow.utils.dates import days_ago

from airflow.utils import trigger_rule

from airflow.contrib.operators import dataproc_operator

from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

PROJECT_ID = models.Variable.get('gcp_project_id')
BUCKET_NAME = PROJECT_ID
REGION = models.Variable.get('gcp_region')
ZONE = models.Variable.get('gcp_zone')

DATAPROC_CLUSTER_NAME = models.Variable.get('dataproc_cluster_name')
DATAPROC_MASTER_MACHINE_TYPE = models.Variable.get('dataproc_master_machine_type')
IMAGE_VERSION = models.Variable.get('image_version')

MAIN_JAR_FILE_URL = "gs://"+BUCKET_NAME+"/store_sales_2.12-0.1.jar"
DEPENDENCY_JAR_FILES = [
    "gs://"+BUCKET_NAME+"/jars/api-common-1.10.1.jar",
    "gs://"+BUCKET_NAME+"/jars/auto-value-annotations-1.7.4.jar",
    "gs://"+BUCKET_NAME+"/jars/checker-compat-qual-2.5.5.jar",
    "gs://"+BUCKET_NAME+"/jars/error_prone_annotations-2.6.0.jar",
    "gs://"+BUCKET_NAME+"/jars/failureaccess-1.0.1.jar",
    "gs://"+BUCKET_NAME+"/jars/gax-1.63.0.jar",
    "gs://"+BUCKET_NAME+"/jars/gax-httpjson-0.80.0.jar",
    "gs://"+BUCKET_NAME+"/jars/google-api-client-1.31.4.jar",
    "gs://"+BUCKET_NAME+"/jars/google-api-services-storage-v1-rev20210127-1.31.0.jar",
    "gs://"+BUCKET_NAME+"/jars/google-auth-library-credentials-0.25.3.jar",
    "gs://"+BUCKET_NAME+"/jars/google-auth-library-oauth2-http-0.25.3.jar",
    "gs://"+BUCKET_NAME+"/jars/google-cloud-core-1.94.7.jar",
    "gs://"+BUCKET_NAME+"/jars/google-cloud-core-http-1.94.7.jar",
    "gs://"+BUCKET_NAME+"/jars/google-cloud-storage-1.113.16.jar",
    "gs://"+BUCKET_NAME+"/jars/google-http-client-1.39.2.jar",
    "gs://"+BUCKET_NAME+"/jars/google-http-client-apache-v2-1.39.2.jar",
    "gs://"+BUCKET_NAME+"/jars/google-http-client-appengine-1.39.2.jar",
    "gs://"+BUCKET_NAME+"/jars/google-http-client-gson-1.39.2.jar",
    "gs://"+BUCKET_NAME+"/jars/google-http-client-jackson2-1.39.2.jar",
    "gs://"+BUCKET_NAME+"/jars/google-oauth-client-1.31.5.jar",
    "gs://"+BUCKET_NAME+"/jars/grpc-context-1.37.0.jar",
    "gs://"+BUCKET_NAME+"/jars/gson-2.8.6.jar",
    "gs://"+BUCKET_NAME+"/jars/guava-30.1.1-android.jar",
    "gs://"+BUCKET_NAME+"/jars/j2objc-annotations-1.3.jar",
    "gs://"+BUCKET_NAME+"/jars/jackson-core-2.12.2.jar",
    "gs://"+BUCKET_NAME+"/jars/javax.annotation-api-1.3.2.jar",
    "gs://"+BUCKET_NAME+"/jars/jsr305-3.0.2.jar",
    "gs://"+BUCKET_NAME+"/jars/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar",
    "gs://"+BUCKET_NAME+"/jars/opencensus-api-0.28.0.jar",
    "gs://"+BUCKET_NAME+"/jars/opencensus-contrib-http-util-0.28.0.jar",
    "gs://"+BUCKET_NAME+"/jars/proto-google-common-protos-2.1.0.jar",
    "gs://"+BUCKET_NAME+"/jars/proto-google-iam-v1-1.0.12.jar",
    "gs://"+BUCKET_NAME+"/jars/protobuf-java-3.15.8.jar",
    "gs://"+BUCKET_NAME+"/jars/protobuf-java-util-3.15.8.jar",
    "gs://"+BUCKET_NAME+"/jars/threetenbp-1.5.0.jar",]

LOCATIONS_STAGING_SPARK_JOB = {
    "placement": {
        "cluster_name": DATAPROC_CLUSTER_NAME
    },
    "reference": {
        "project_id": PROJECT_ID
    },
    "spark_job": {
        "main_jar_file_uri": MAIN_JAR_FILE_URL,
        "jar_file_uris": DEPENDENCY_JAR_FILES,
        "properties": {},
        "args": [
            PROJECT_ID,
            "stage-locations"
        ]
    }
}

PRODUCTS_STAGING_SPARK_JOB = {
    "placement": {
        "cluster_name": DATAPROC_CLUSTER_NAME
    },
    "reference": {
        "project_id": PROJECT_ID
    },
    "spark_job": {
        "main_jar_file_uri": MAIN_JAR_FILE_URL,
        "jar_file_uris": DEPENDENCY_JAR_FILES,
        "properties": {},
        "args": [
            PROJECT_ID,
            "stage-products"
        ]
    }
}

TRANSACTIONS_STAGING_SPARK_JOB = {
    "placement": {
        "cluster_name": DATAPROC_CLUSTER_NAME
    },
    "reference": {
        "project_id": PROJECT_ID
    },
    "spark_job": {
        "main_jar_file_uri": MAIN_JAR_FILE_URL,
        "jar_file_uris": DEPENDENCY_JAR_FILES,
        "properties": {},
        "args": [
            PROJECT_ID,
            "stage-transactions"
        ]
    }
}

ENRICH_STAGING_SPARK_JOB = {
    "placement": {
        "cluster_name": DATAPROC_CLUSTER_NAME
    },
    "reference": {
        "project_id": PROJECT_ID
    },
    "spark_job": {
        "main_jar_file_uri": MAIN_JAR_FILE_URL,
        "jar_file_uris": DEPENDENCY_JAR_FILES,
        "properties": {},
        "args": [
            PROJECT_ID,
            "enrich"
        ]
    }
}

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

with models.DAG(
    'acme_sales_staging_composer',
    schedule_interval = datetime.timedelta(days=1),
    default_args=default_dag_args) as dag:

    create_dataproc_acme_sales_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_acme_sales_cluster',
        cluster_name=DATAPROC_CLUSTER_NAME,
        region=REGION,
        zone=ZONE,
        num_workers=3,
        master_machine_type=DATAPROC_MASTER_MACHINE_TYPE,
        worker_machine_type=DATAPROC_MASTER_MACHINE_TYPE,
        image_version=IMAGE_VERSION,
        project_id=PROJECT_ID)

    locations_staging_spark_job = DataprocSubmitJobOperator(
        task_id="locations_staging_spark_job",
        job=LOCATIONS_STAGING_SPARK_JOB,
        location=REGION,
        project_id=PROJECT_ID)

    products_staging_spark_job = DataprocSubmitJobOperator(
        task_id="products_staging_spark_job",
        job=PRODUCTS_STAGING_SPARK_JOB,
        location=REGION,
        project_id=PROJECT_ID)

    transactions_staging_spark_job = DataprocSubmitJobOperator(
        task_id="transactions_staging_spark_job",
        job=TRANSACTIONS_STAGING_SPARK_JOB,
        location=REGION,
        project_id=PROJECT_ID)

    enrich_staging_spark_job = DataprocSubmitJobOperator(
        task_id="enrich_staging_spark_job",
        job=ENRICH_STAGING_SPARK_JOB,
        location=REGION,
        project_id=PROJECT_ID)

    delete_dataproc_acme_sales_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id="delete_dataproc_acme_sales_cluster",
        cluster_name=DATAPROC_CLUSTER_NAME,
        region=REGION,
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
        project_id=PROJECT_ID)

    create_dataproc_acme_sales_cluster >> [locations_staging_spark_job,products_staging_spark_job,transactions_staging_spark_job] >> enrich_staging_spark_job >> delete_dataproc_acme_sales_cluster

if __name__ == '__main__':
    dag.clear(dag_run_state=State.NONE)
    dag.run()
