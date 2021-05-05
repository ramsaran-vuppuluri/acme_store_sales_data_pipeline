package ca.acme.store

/**
 * @author Ram Saran Vuppuluri.
 *
 *         This Scala object is the entry point for the data pipeline. This object will take a string of valid arguments
 *         and perform the operations accordingly
 *
 *         The first argument that is passed is the projectId. The same name is used by the bucket to ensure uniqueness
 *         and consistency.
 *
 *         The second argument that is passed is the action, following are the valid actions that are available:
 *
 *         * stage-locations - Will process the location.csv file in the landing zone and persist in the staging zone.
 *         The data is partitioned at province level in staging zone. Original file is moved to archive zone and the
 *         copy from landing zone is deleted.
 *
 *         When a new location.csv file is processed to staging zone the current file is overwritten.
 *
 *         * stage-products - Will process the product.csv file in the landing zone and persist in the staging zone. The
 *         data is partitioned at the department level in staging zone. Original file is moved to archive zone and the
 *         copy from landing zone is deleted.
 *
 *         When a new product.csv file is processed to staging zone the current file is appended.
 *
 *         * stage-transactions - Will process the files in landing zone matching the "trans_fact_*.csv" pattern and
 *         persist the processed data in the staging zone. The transactions are partitioned at year and month level.
 *         Original file is moved to archive zone and the copy from landing zone is deleted.
 *
 *         When a new trans_fact_*.csv file is processed to staging zone the current file is appended.
 *
 *         * enrich - Will join the transactions data with products & locations data from staging region and persist the
 *         consolidated data to enrichment zone. Data is partitioned at province, year and month level.
 *
 *         All the joins are left joins to not loose any data.
 *
 *         * reporting - Will read consolidated data from enrichment zone and generate analysis reports as standard
 *         outputs. For visualization, aggregated data is stored as csv's.
 *
 *         All the output files are of parquet format with snappy compression.
 *
 *         Build command - sbt clean publishLocal
 */

import ca.acme.store.enrich.EnrichStoreSales
import ca.acme.store.reporting.GenerateReports
import ca.acme.store.staging.{StageLocationsData, StageProductsData, StageTransactionsData}
import org.apache.spark.sql.SparkSession

object Driver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("acme_store_staging").getOrCreate()

    /**
     * GCP project ID.
     */
    val projectId = args(0)

    /**
     * Bucket name in which data is persisted. This is same value as projectId to ensure uniqueness and consistency.
     */
    val bucketName = projectId

    args(1) match {
      case "stage-locations" => {
        /*
         * Will process the location.csv file in the landing zone and persist in the staging zone.The data is partitioned
         * at province level in staging zone. Original file is moved to archive zone and the copy from landing zone is deleted.
         *
         * When a new location.csv file is processed to staging zone the current file is overwritten.
         */
        StageLocationsData(spark, bucketName)
      }
      case "stage-products" => {
        /*
         * Will process the product.csv file in the landing zone and persist in the staging zone. The data is partitioned
         * at the department level in staging zone. Original file is moved to archive zone and the copy from landing zone
         * is deleted.
         *
         * When a new product.csv file is processed to staging zone the current file is appended.
         */
        StageProductsData(spark, bucketName)
      }
      case "stage-transactions" => {
        /*
         * Will process the files in landing zone matching the "trans_fact_*.csv" pattern and persist the processed data
         * in the staging zone. The transactions are partitioned at year and month level. Original file is moved to
         * archive zone and the copy from landing zone is deleted.
         *
         * When a new trans_fact_*.csv file is processed to staging zone the current file is appended.
         */
        StageTransactionsData(spark, bucketName)
      }
      case "enrich" => {
        /**
         * Will join the transactions data with products & locations data from staging region and persist the consolidated
         * data to enrichment zone. Data is partitioned at province, year and month level.
         *
         * All the joins are left joins to not loose any data.
         */
        EnrichStoreSales(spark, bucketName)
      }
      case "reporting" => {
        /**
         * Will read consolidated data from enrichment zone and generate analysis reports as standard outputs. For
         * visualization, aggregated data is stored as csv's.
         *
         * All the output files are of parquet format with snappy compression.
         */
        GenerateReports(spark, bucketName)
      }
    }
  }
}
