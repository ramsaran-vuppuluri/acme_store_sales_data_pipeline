package ca.acme.store.staging

/**
 * @author Ram Saran Vuppuluri.
 *
 *         This object will read the the product.csv file in the landing zone and persist in the staging zone. The data
 *         is partitioned at the department level in staging zone. Original file is moved to archive zone and the copy
 *         from landing zone is deleted.
 *
 *         When a new product.csv file is processed to staging zone the current file is appended.
 */

import java.time.LocalDateTime

import ca.acme.store.generic.GcsIO
import ca.acme.store.schema.ProductsLandingSchema
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

object StageProductsData extends GcsIO {
  /**
   * This method will perform the core transformations of products data that is read from landing zone before persisting
   * into the staging zone.
   *
   * @param spark      Instance of spark session.
   * @param bucketName Bucket name in which data is processed in.
   * @param objectPath Object path, by default it will be "landing/product.csv"
   */
  def apply(spark: SparkSession, bucketName: String, objectPath: String = "landing/product.csv"): Unit = {
    /*
     * This common method will read the CSV files from GCS.
     */

    /*
     * +-----------+---+-----------+---------+----------------+----------+--------+
     * |product_key|sku|upc        |item_name|item_description|department|category|
     * +-----------+---+-----------+---------+----------------+----------+--------+
     * |7652613339 |0  |7652613339 |324168   |NA              |651b1068  |8312aed6|
     * |1810063322 |0  |1810063322 |276973   |NA              |651b1068  |7aaa7a34|
     * |5274585486 |0  |5274585486 |794396   |NA              |c81ba571  |54ea8364|
     * |6978362094 |0  |6978362094 |510386   |NA              |b947a4a9  |382cf3a |
     * |6978396053 |0  |6978396053 |120105   |NA              |b947a4a9  |382cf3a |
     * |6978396523 |0  |6978396523 |271194   |NA              |b947a4a9  |382cf3a |
     * |6978371194 |0  |6978371194 |283130   |NA              |b947a4a9  |382cf3a |
     * |77805452601|0  |77805452601|61311    |NA              |5bffa719  |4210bb19|
     * |5468208917 |0  |5468208917 |885995   |NA              |651b1068  |b60187d0|
     * |6010796869 |0  |6010796869 |952237   |NA              |651b1068  |d0547922|
     * |6976200706 |0  |6976200706 |136995   |NA              |651b1068  |7ac5490 |
     * |77477400030|0  |77477400030|55624    |NA              |5bffa719  |8f610228|
     * |9500000289 |0  |9500000289 |726939   |NA              |b947a4a9  |3b380e17|
     * |77928840001|0  |77928840001|334256   |NA              |1a34cbb9  |98417e80|
     * |60793908205|0  |60793908205|944638   |NA              |4b8d7c31  |ad525786|
     * |6574308386 |0  |6574308386 |367908   |NA              |4b8d7c31  |2588bef |
     * |80757542001|0  |80757542001|159668   |NA              |651b1068  |d0547922|
     * |60249824386|0  |60249824386|609569   |NA              |651b1068  |b7dbc305|
     * |7073407011 |0  |7073407011 |258697   |NA              |1a34cbb9  |7b703ee1|
     * |6285157064 |0  |6285157064 |952363   |NA              |c81ba571  |54ea8364|
     * +-----------+---+-----------+---------+----------------+----------+--------+
     * only showing top 20 rows
     */
    val productsDF = readFromCSV(spark, bucketName, objectPath)

    println(s"gs://$bucketName/$objectPath read by Spark")

    val productsDFColumns = ProductsLandingSchema.schema.map {
      ele => ele.name
    }

    val productsDFToWrite = productsDF
      .withColumn("product_key", col("product_key").cast(StringType))
      .withColumn("sku", col("sku").cast(StringType))
      .withColumn("upc", col("upc").cast(StringType))
      .withColumn("item_name", col("item_name").cast(StringType))
      .withColumn("item_description", col("item_description").cast(StringType))
      .withColumn("department", col("department").cast(StringType))
      .withColumn("category", col("category").cast(StringType))
      .selectExpr(productsDFColumns: _*)

    /*
     * This common method will write the dataframe into GCS in parquet format with snappy compression.
     */
    writeToParquet(productsDFToWrite, SaveMode.Append, ProductsLandingSchema.partitionColumns, s"gs://$bucketName/staging/product/")

    println(s"gs://$bucketName/staging/product/ written by Spark")

    /*
     * This common method will write the dataframe into GCS in CSV format without any partitions.
     */
    writeToCSV(productsDF, SaveMode.Overwrite, s"gs://$bucketName/archive/landing/product/${LocalDateTime.now()}/")

    println(s"gs://$bucketName/$objectPath archived by Spark")

    /*
     * This method will delete the object in the bucket using the GCP IO API.
     */
    deleteObjectFromBucket(bucketName, objectPath)
  }
}
