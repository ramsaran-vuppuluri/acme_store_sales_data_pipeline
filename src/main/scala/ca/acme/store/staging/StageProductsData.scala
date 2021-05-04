package ca.acme.store.staging

import java.time.LocalDateTime

import ca.acme.store.generic.GcsIO
import ca.acme.store.schema.ProductsLandingSchema
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType

object StageProductsData extends GcsIO {
  def apply(spark: SparkSession, bucketName: String, objectName: String = "landing/product.csv"): Unit = {
    val productsDF = readFromCSV(spark, bucketName, objectName)

    println(s"gs://$bucketName/$objectName read by Spark")

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

    productsDFToWrite.write
      .mode(SaveMode.Append)
      .partitionBy(ProductsLandingSchema.partitionColumns: _*)
      .parquet(s"gs://$bucketName/staging/product/")

    println(s"gs://$bucketName/staging/product/ written by Spark")

    productsDF.write
      .option("header", "true")
      .csv(s"gs://$bucketName/archive/landing/product/${LocalDateTime.now()}/")

    println(s"gs://$bucketName/$objectName archived by Spark")

    deleteObjectFromBucket(bucketName, objectName)
  }
}
