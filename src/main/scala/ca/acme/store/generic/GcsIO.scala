package ca.acme.store.generic

import com.google.cloud.storage.{Storage, StorageOptions}
import org.apache.spark.sql.{DataFrame, SparkSession}

trait GcsIO {
  val storage: Storage = StorageOptions.getDefaultInstance.getService()

  def readFromCSV(spark: SparkSession, bucketName: String, objectName: String): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"gs://$bucketName/$objectName")
  }

  def readFromParquet(spark: SparkSession, bucketName: String, objectName: String): DataFrame = {
    spark.read.parquet(s"gs://$bucketName/$objectName")
  }


  def deleteObjectFromBucket(bucketName: String, objectName: String): Unit = {
    println(s"Deleting gs://$bucketName/$objectName file.")

    storage.delete(bucketName, objectName)

    println(s"Deleted gs://$bucketName/$objectName file.")
  }
}
