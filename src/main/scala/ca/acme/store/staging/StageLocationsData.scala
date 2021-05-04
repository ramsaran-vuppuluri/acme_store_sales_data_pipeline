package ca.acme.store.staging

/**
 * @author Ram Saran Vuppuluri
 *
 *         This object will read the location.csv placed in the GCS
 */

import java.time.LocalDateTime

import ca.acme.store.generic.GcsIO
import ca.acme.store.schema.LocationsStagingSchema
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

object StageLocationsData extends GcsIO {
  def apply(spark: SparkSession, bucketName: String, objectName: String = "landing/location.csv"): Unit = {
    val locationDF = readFromCSV(spark, bucketName, objectName)

    println(s"gs://$bucketName/$objectName read by Spark")

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

    /**
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

    /**
     * Change the save mode to Append if we get only new Location data.
     */
    locationDFToWrite.write
      .mode(SaveMode.Overwrite)
      .partitionBy(LocationsStagingSchema.partitionColumns: _*)
      .parquet(s"gs://$bucketName/staging/location/")

    println(s"gs://$bucketName/staging/location/ written by Spark")

    locationDF.write
      .option("header", "true")
      .csv(s"gs://$bucketName/archive/landing/location/${LocalDateTime.now()}/")

    println(s"gs://$bucketName/landing/location.csv archived by Spark")

    deleteObjectFromBucket(bucketName, objectName)
  }
}