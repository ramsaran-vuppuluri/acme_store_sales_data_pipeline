package ca.acme.store

import ca.acme.store.enrich.EnrichStoreSales
import ca.acme.store.reporting.GenerateReports
import ca.acme.store.staging.{StageLocationsData, StageProductsData, StageTransactionsData}
import org.apache.spark.sql.SparkSession

//sbt clean package
//sbt clean publishLocal
object Driver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("acme_store_staging").getOrCreate()

    val projectId = args(0)
    val bucketName = projectId

    args(1) match {
      case "stage-locations" => {
        StageLocationsData(spark, bucketName)
      }
      case "stage-products" => {
        StageProductsData(spark, bucketName)
      }
      case "stage-transactions" => {
        StageTransactionsData(spark, bucketName)
      }
      case "enrich" => {
        EnrichStoreSales(spark, bucketName)
      }
      case "reporting" => {
        GenerateReports(spark, bucketName)
      }
    }
  }
}
