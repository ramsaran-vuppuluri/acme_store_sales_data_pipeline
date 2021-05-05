package ca.acme.store.staging

/**
 * @author Ram Saran Vuppuluri
 *
 *         This object will read the location.csv file in the landing zone and persist in the staging zone.The data is
 *         partitioned at province level in staging zone. Original file is moved to archive zone and the copy from landing
 *         zone is deleted.
 *
 *         When a new location.csv file is processed to staging zone the current file is overwritten.
 */

import java.time.LocalDateTime

import ca.acme.store.generic.GcsIO
import ca.acme.store.schema.LocationsStagingSchema
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

object StageLocationsData extends GcsIO {
  /**
   * This method will perform the core transformations of locations data that is read from landing zone before persisting
   * into the staging zone.
   *
   * @param spark      Instance of spark session.
   * @param bucketName Bucket name in which data is processed in.
   * @param objectPath Object path, by default it will be "landing/location.csv"
   */
  def apply(spark: SparkSession, bucketName: String, objectPath: String = "landing/location.csv"): Unit = {
    /*
     * This common method will read the CSV files from GCS.
     */

    /*
     * +------------------+------+----------------+-----------+-----------+--------+---------+
     * |store_location_key|region|province        |city       |postal_code|banner  |store_num|
     * +------------------+------+----------------+-----------+-----------+--------+---------+
     * |1794              |NA    |ONTARIO         |LONDON     |NA         |dc35b695|1794     |
     * |42                |NA    |ONTARIO         |TORONTO    |NA         |dc35b695|42       |
     * |7218              |NA    |ALBERTA         |SLAVE LAKE |NA         |6e36648 |7218     |
     * |7196              |NA    |BRITISH COLUMBIA|VICTORIA   |NA         |6e36648 |7196     |
     * |7167              |NA    |BRITISH COLUMBIA|SURREY     |NA         |6e36648 |7167     |
     * |7518              |NA    |ALBERTA         |STONEWALL  |NA         |6e36648 |7518     |
     * |2022              |NA    |ONTARIO         |OTTAWA     |NA         |dc35b695|2022     |
     * |8611              |NA    |ALBERTA         |DRUMHELLER |NA         |235704e1|8611     |
     * |5023              |NA    |ONTARIO         |BARRIE     |NA         |b2e9116e|5023     |
     * |8101              |NA    |ONTARIO         |LONDON     |NA         |6e36648 |8101     |
     * |7202              |NA    |ALBERTA         |WHITECOURT |NA         |6e36648 |7202     |
     * |1388              |NA    |ONTARIO         |BARRIE     |NA         |dc35b695|1388     |
     * |7262              |NA    |ALBERTA         |CALGARY    |NA         |6e36648 |7262     |
     * |7315              |NA    |SASKATCHEWAN    |MOOSOMIN   |NA         |6e36648 |7315     |
     * |8134              |NA    |ONTARIO         |OTTAWA     |NA         |6e36648 |8134     |
     * |18                |NA    |ONTARIO         |TORONTO    |NA         |dc35b695|18       |
     * |8544              |NA    |ONTARIO         |LONDON     |NA         |235704e1|8544     |
     * |7263              |NA    |ALBERTA         |CALGARY    |NA         |6e36648 |7263     |
     * |9607              |NA    |ONTARIO         |KINGSTON   |NA         |6e36648 |9607     |
     * |2340              |NA    |ONTARIO         |ORANGEVILLE|NA         |dc35b695|2340     |
     * +------------------+------+----------------+-----------+-----------+--------+---------+
     * only showing top 20 rows
     */
    val locationDF = readFromCSV(spark, bucketName, objectPath)

    println(s"gs://$bucketName/$objectPath read by Spark")

    val locationDFColumns = LocationsStagingSchema.schema.map {
      ele => ele.name
    }

    val locationDFToWrite = locationDF
      .withColumn("store_location_key", col("store_location_key").cast(StringType))
      .withColumn("region", col("region").cast(StringType))
      .withColumn("province", col("province").cast(StringType))
      .withColumn("city", col("city").cast(StringType))
      .withColumn("postal_code", col("postal_code").cast(StringType))
      .withColumn("banner", col("banner").cast(StringType))
      .withColumn("store_num", col("store_num").cast(StringType))
      .selectExpr(locationDFColumns: _*)



    /*
     * Change the save mode to Append if we get only new Location data.
     */

    /*
     * This common method will write the dataframe into GCS in parquet format with snappy compression.
     */
    writeToParquet(locationDFToWrite, SaveMode.Overwrite, LocationsStagingSchema.partitionColumns, s"gs://$bucketName/staging/location/")

    println(s"gs://$bucketName/staging/location/ written by Spark")

    /*
     * This common method will write the dataframe into GCS in CSV format without any partitions.
     */
    writeToCSV(locationDF, SaveMode.Overwrite, s"gs://$bucketName/archive/landing/location/${LocalDateTime.now()}/")

    println(s"gs://$bucketName/landing/location.csv archived by Spark")

    /*
     * This method will delete the object in the bucket using the GCP IO API.
     */
    deleteObjectFromBucket(bucketName, objectPath)
  }
}