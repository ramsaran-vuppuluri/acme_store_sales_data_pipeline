package ca.acme.store.generic

/**
 * @author Ram Saran Vuppuluri
 *
 *         This trait contains the common IO code to interact with GCS either via spark or using the GCP API.
 */

import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Blob, Storage, StorageOptions}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

trait GcsIO {
  /*
   * Instance of Storage object to authenticate with GCP to access GCS resources. By default this code is designed to run
   * only on the GCP workspace as this code cannot consume the authentication keys.ß
   */
  private val storage: Storage = StorageOptions.getDefaultInstance.getService()

  /**
   * This common method will read the CSV files from GCS.
   *
   * @param spark      Instance of spark session.
   * @param bucketName Bucket name in which data is processed in.
   * @param objectPath objectPath of the csv file.
   *
   * @return
   */
  protected def readFromCSV(spark: SparkSession, bucketName: String, objectPath: String): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(s"gs://$bucketName/$objectPath")
  }

  /**
   * This method will read from parquet files and generate Spark DataFrame.
   *
   * @param spark      Instance of spark session.
   * @param bucketName Bucket name in which data is processed in.
   * @param objectPath Object path of the parquet file.
   *
   * @return
   */
  protected def readFromParquet(spark: SparkSession, bucketName: String, objectPath: String): DataFrame = {
    spark.read.parquet(s"gs://$bucketName/$objectPath")
  }

  /**
   * This common method will write the dataframe into GCS in CSV format without any partitions.
   *
   * @param dataFrame       Dataframe instance.
   * @param mode            Save Mode
   * @param objectWritePath Path at which dataframe should be persisted.
   */
  protected def writeToCSV(dataFrame: DataFrame, mode: SaveMode, objectWritePath: String): Unit = {
    dataFrame.write
      .option("header", "true")
      .mode(mode)
      .csv(objectWritePath)
  }

  /**
   * This common method will write the dataframe into GCS in parquet format with snappy compression.
   *
   * @param dataFrame        Dataframe instance.
   * @param mode             Save Mode
   * @param partitionColumns Sequence of partition columns
   * @param objectWritePath  Path at which dataframe should be persisted.
   */
  protected def writeToParquet(dataFrame: DataFrame, mode: SaveMode, partitionColumns: Seq[String], objectWritePath: String): Unit = {
    dataFrame.write
      .mode(mode)
      .partitionBy(partitionColumns: _*)
      .parquet(objectWritePath)
  }

  /**
   * This method will return the list of files in the path.
   *
   * @param bucketName Bucket name in which data is processed in.
   * @param pathPrefix Relative path prefix.
   *
   * @return Sequence of pagenated Blob instances.
   */
  protected def getListOfFilesFromPath(bucketName: String, pathPrefix: String): Page[Blob] = {
    storage.list(bucketName, Storage.BlobListOption.prefix(pathPrefix))
  }

  /**
   * This method will delete the object in the bucket using the GCP IO API.
   *
   * @param bucketName Bucket name in which data is processed in.
   * @param objectName Object name.
   */
  protected def deleteObjectFromBucket(bucketName: String, objectName: String): Unit = {
    println(s"Deleting gs://$bucketName/$objectName file.")

    storage.delete(bucketName, objectName)

    println(s"Deleted gs://$bucketName/$objectName file.")
  }
}
