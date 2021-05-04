package ca.acme.store.staging

import java.text.SimpleDateFormat
import java.time.LocalDateTime

import ca.acme.store.generic.GcsIO
import ca.acme.store.schema.TransactionStagingSchema
import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Blob, Storage, StorageOptions}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, month, to_date, when, year, udf}
import org.apache.spark.sql.types.{DateType, DecimalType, DoubleType, IntegerType, LongType, StringType}

object StageTransactionsData extends GcsIO {
  def apply(spark: SparkSession, bucketName: String, objectName: String = "landing/trans_fact_*.csv"): Unit = {
    val stagingColumns = TransactionStagingSchema.schema.map {
      ele => ele.name
    }

    val storage: Storage = StorageOptions.getDefaultInstance.getService()

    val blobs: Page[Blob] = storage.list(bucketName, Storage.BlobListOption.prefix("landing/"))

    var transactionFactsDF = spark.sqlContext.createDataFrame(
      spark.sparkContext.emptyRDD[Row], TransactionStagingSchema.schema)

    blobs.iterateAll().forEach {
      blob: Blob => {
        if (blob.getName.startsWith("landing/trans_fact_")) {
          val df = appendTransactionFacts(spark, bucketName, blob)

          transactionFactsDF = transactionFactsDF.selectExpr(stagingColumns: _*)
            .union(df.selectExpr(stagingColumns: _*))
        }
      }
    }

    transactionFactsDF
      .write
      .mode(SaveMode.Append)
      .partitionBy(TransactionStagingSchema.partitionColumns: _*)
      .parquet(s"gs://$bucketName/staging/transactions/")

    blobs.iterateAll().forEach {
      blob: Blob => {
        if (blob.getName.startsWith("landing/trans_fact_")) {
          deleteObjectFromBucket(bucketName, blob.getName)
        }
      }
    }
  }

  private def appendTransactionFacts(spark: SparkSession, bucketName: String, ele: Blob): DataFrame = {
    val df = readFromCSV(spark, bucketName, ele.getName)

    var transactionFactsDF = df

    println(s"gs://$bucketName//landing/${ele.getName} read by Spark")

    val parseTransDt = (strDate: String) => {
      new SimpleDateFormat("MM/dd/yyyy").format(new SimpleDateFormat("MM/dd/yyyy").parse(strDate))
    }

    val parseTransDtUDF = udf(parseTransDt)

    transactionFactsDF = transactionFactsDF
      .withColumn("store_location_key", col("store_location_key").cast(StringType))
      .withColumn("product_key", col("product_key").cast(StringType))
      .withColumn("collector_key", col("collector_key").cast(StringType))
      .withColumn("trans_dt_1", col("trans_dt").cast(DateType))
      .withColumn("trans_dt", when(col("trans_dt_1").isNull, to_date(parseTransDtUDF(col("trans_dt")), "MM/dd/yyyy")).otherwise(col("trans_dt_1")))
      .withColumn("sales", col("sales").cast(DoubleType))
      .withColumn("units", col("units").cast(IntegerType))
      .withColumn("trans_year", year(col("trans_dt")))
      .withColumn("trans_month", month(col("trans_dt")))

    if (transactionFactsDF.columns.contains("trans_id")) {
      transactionFactsDF = transactionFactsDF.withColumn("trans_key", col("trans_id").cast(DecimalType(30, 0)).cast(StringType))
    } else {
      transactionFactsDF = transactionFactsDF.withColumn("trans_key", col("trans_key").cast(DecimalType(30, 0)).cast(StringType))
    }

    transactionFactsDF = transactionFactsDF
      .na.fill(0.0, Array("sales"))
      .na.fill(0, Array("units"))

    df.write
      .option("header", "true")
      .csv(s"gs://$bucketName/archive/landing/transactions/${LocalDateTime.now()}/")

    println(s"gs://$bucketName/landing/${ele.getName} archived by Spark")

    transactionFactsDF
  }
}