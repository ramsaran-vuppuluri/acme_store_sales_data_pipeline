package ca.acme.store.enrich

import ca.acme.store.generic.GcsIO
import ca.acme.store.schema.TransactionEnrichmentSchema
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object EnrichStoreSales extends GcsIO {
  def apply(spark: SparkSession, bucketName: String): Unit = {
    val locationDFIn = readFromParquet(spark, bucketName, "staging/location/")
    val productsDFIn = readFromParquet(spark, bucketName, "staging/product/")
    val transactionsDF = readFromParquet(spark, bucketName, "staging/transactions/")

    println(s"Staging locationDF read count :: ${locationDFIn.count}")
    println(s"Staging productsDF read count :: ${productsDFIn.count}")
    println(s"Staging transactionsDF read count :: ${transactionsDF.count}")

    val locationDF = locationDFIn
      .withColumnRenamed("store_location_key", "store_location_key_left")

    val productsDF = productsDFIn
      .withColumnRenamed("product_key", "product_key_left")

    var enrichDF: DataFrame = transactionsDF.join(productsDF,
      transactionsDF("product_key") === productsDF("product_key_left"),
      "left").drop("product_key_left")

    println(s"enrichDF count after joining transactionsDF with productsDF :: ${enrichDF.count}")

    enrichDF = enrichDF.join(locationDF,
      enrichDF("store_location_key") === locationDF("store_location_key_left"),
      "left").drop("store_location_key_left")

    println(s"enrichDF count after joining enrichDF with locationDF :: ${enrichDF.count}")

    val columns = TransactionEnrichmentSchema.schema.map {
      ele => ele.name
    }

    enrichDF = enrichDF
      .selectExpr(columns: _*)

    enrichDF
      .write
      .partitionBy(TransactionEnrichmentSchema.partitionColumns: _*)
      .mode(SaveMode.Append)
      .parquet(s"gs://$bucketName/enrichment/transactions/")
  }
}
