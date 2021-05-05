package ca.acme.store.staging

/**
 * @author Ram Saran Vuppuluri.
 *
 *         This object will read the files in landing zone matching the "trans_fact_*.csv" pattern and persist the processed
 *         data in the staging zone. The transactions are partitioned at year and month level.
 *
 *         Original file is moved to archive zone and the copy from landing zone is deleted.
 *
 *         When a new trans_fact_*.csv file is processed to staging zone the current file is appended.
 */

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
  /**
   * This method will perform the core transformations of transaction data that is read from landing zone before persisting
   * into the staging zone.
   *
   * @param spark            Instance of spark session.
   * @param bucketName       Bucket name in which data is processed in.
   * @param objectPathPrefix Object path, by default it will be "landing/"
   */
  def apply(spark: SparkSession, bucketName: String, objectPathPrefix: String = "landing/"): Unit = {
    /*
     * This method will return the list of files in the path.
     */
    val blobs: Page[Blob] = getListOfFilesFromPath(bucketName, objectPathPrefix)

    var transactionFactsDF = spark.sqlContext.createDataFrame(spark.sparkContext.emptyRDD[Row], TransactionStagingSchema.schema)

    val stagingColumns = TransactionStagingSchema.schema.map {
      ele => ele.name
    }

    /**
     * This code snippet will iterate through the files in the path and filter file names starting with landing/trans_fact_.
     * Each of the transaction fact file is processed individually and cleaned dataframes are unioned to persist in the
     * Staging zone as one single transaction file.
     */
    blobs.iterateAll().forEach {
      blob: Blob => {
        if (blob.getName.startsWith("landing/trans_fact_")) {
          /*
           * This method will transform the transaction facts to address data quality issues in individual files.
           */
          val df = transformTransactionFacts(spark, bucketName, blob)

          transactionFactsDF = transactionFactsDF.selectExpr(stagingColumns: _*)
            .union(df.selectExpr(stagingColumns: _*))
        }
      }
    }

    /*
     * This common method will write the dataframe into GCS in parquet format with snappy compression.
     */
    writeToParquet(transactionFactsDF, SaveMode.Append, TransactionStagingSchema.partitionColumns, s"gs://$bucketName/staging/transactions/")

    blobs.iterateAll().forEach {
      blob: Blob => {
        if (blob.getName.startsWith("landing/trans_fact_")) {
          /*
           * This method will delete the object in the bucket using the GCP IO API.
           */
          deleteObjectFromBucket(bucketName, blob.getName)
        }
      }
    }
  }

  /**
   * This method will transform the transaction facts to address data quality issues in individual files.
   *
   * @param spark      Spark Session instance.
   * @param bucketName Bucket name
   * @param ele        Instance of Blob matching the landing/trans_fact_ pattern.
   *
   * @return Clean transaction facts data frame.
   */
  private def transformTransactionFacts(spark: SparkSession, bucketName: String, ele: Blob): DataFrame = {
    /*
     * This common method will read the CSV files from GCS.
     */

    /*
     * The following data quality issues were observed when analyzing the transaction_fact*.csv files.
     *
     * 1. Date format of trans_dt is not uniform.
     *
     * File name trans_fact_5.csv
     * +-------------+-------------------+------------------+---------------+-----+-----+--------------------------+
     * |collector_key|trans_dt           |store_location_key|product_key    |sales|units|trans_key                 |
     * +-------------+-------------------+------------------+---------------+-----+-----+--------------------------+
     * |-1           |2015-10-28 00:00:00|6973              |999999999999513|0.0  |1    |31575569731182201510281142|
     * |-1           |2015-07-24 00:00:00|6973              |77105810883    |8.53 |1    |31216969731182201507241448|
     * |141178981825 |2015-12-14 00:00:00|6973              |30041060552    |11.55|1    |31755569731182201512141140|
     * |-1           |2015-04-10 00:00:00|6973              |999999999999142|0.0  |1    |30763169731182201504101230|
     * |139518403300 |2015-10-14 00:00:00|6973              |999999999999513|0.0  |1    |3152046973118220151014924 |
     * |-1           |2015-07-30 00:00:00|6973              |999999999999513|0.0  |1    |3124006973118220150730849 |
     * |137298570713 |2015-05-20 00:00:00|6973              |77105880035    |1.58 |1    |30931269731182201505201540|
     * |-1           |2015-12-11 00:00:00|6973              |999999999999513|8.88 |1    |31753069731182201512111455|
     * |-1           |2016-01-08 00:00:00|6973              |62455802158    |32.02|1    |31846069731182201601081000|
     * |-1           |2015-05-22 00:00:00|6973              |999999999999513|73.96|2    |3094486973118220150522856 |
     * +-------------+-------------------+------------------+---------------+-----+-----+--------------------------+
     *
     * File name trans_fact_4.csv
     * +-------------+----------+------------------+-----------+-----+-----+-----------------+
     * |collector_key|trans_dt  |store_location_key|product_key|sales|units|trans_key        |
     * +-------------+----------+------------------+-----------+-----+-----+-----------------+
     * |-1           |6/26/2015 |8142              |4319416816 |9.42 |1    |1.6945500000E+25 |
     * |-1           |10/25/2015|8142              |6210700491 |24.9 |1    |3.4001800000E+25 |
     * |-1           |9/18/2015 |8142              |5873832611 |12.09|1    |1.7274800000E+25 |
     * |-1           |9/14/2015 |8142              |77105810243|20.45|1    |4.145280000E+24  |
     * |-1           |4/18/2015 |8142              |5610007708 |10.31|1    |2.6415800000E+25 |
     * |-1           |7/3/2015  |8142              |78616265005|5.32 |1    |1.69719000000E+26|
     * |-1           |12/11/2015|8142              |4740007371 |42.7 |1    |1.76703000000E+26|
     * |-1           |5/6/2015  |8142              |6454130695 |46.26|1    |2.7260800000E+25 |
     * |-1           |4/1/2015  |8142              |77105810774|17.78|1    |3.287180000E+24  |
     * |-1           |1/2/2016  |8142              |18021700046|64.06|1    |3.6831800000E+25 |
     * +-------------+----------+------------------+-----------+-----+-----+-----------------+
     *
     * 2. There are null values that are present in sales and units.
     *
     * File name trans_fact_6.csv
     * +-------------+----------+------------------+----------------+-----+-----+----------------+
     * |collector_key|trans_dt  |store_location_key|product_key     |sales|units|trans_id        |
     * +-------------+----------+------------------+----------------+-----+-----+----------------+
     * |-1           |10/28/2015|6973              |1000000000000000|0.0  |null |3.1575600000E+25|
     * |-1           |7/24/2015 |6973              |77105810883     |8.53 |null |3.1217000000E+25|
     * |141179000000 |12/14/2015|6973              |30041060552     |11.55|null |3.1755600000E+25|
     * |-1           |4/10/2015 |6973              |1000000000000000|0.0  |null |3.0763200000E+25|
     * |139518000000 |10/14/2015|6973              |1000000000000000|0.0  |null |3.152050000E+24 |
     * |-1           |7/30/2015 |6973              |1000000000000000|0.0  |null |3.124010000E+24 |
     * |137299000000 |5/20/2015 |6973              |77105880035     |1.58 |null |3.0931300000E+25|
     * |-1           |12/11/2015|6973              |1000000000000000|8.88 |null |3.1753100000E+25|
     * |-1           |1/8/2016  |6973              |62455802158     |32.02|null |3.1846100000E+25|
     * |-1           |5/22/2015 |6973              |1000000000000000|73.96|null |3.094490000E+24 |
     * +-------------+----------+------------------+----------------+-----+-----+----------------+
     *
     * File name trans_fact_8.csv
     * +----------------+-------------+----------+------------------+-----+-----+----------------+
     * |product_key     |collector_key|trans_dt  |store_location_key|sales|units|trans_id        |
     * +----------------+-------------+----------+------------------+-----+-----+----------------+
     * |1000000000000000|-1           |10/28/2015|6973              |null |1    |3.1575600000E+25|
     * |77105810883     |-1           |7/24/2015 |6973              |null |1    |3.1217000000E+25|
     * |30041060552     |141179000000 |12/14/2015|6973              |null |1    |3.1755600000E+25|
     * |1000000000000000|-1           |4/10/2015 |6973              |null |1    |3.0763200000E+25|
     * |1000000000000000|139518000000 |10/14/2015|6973              |null |1    |3.152050000E+24 |
     * |1000000000000000|-1           |7/30/2015 |6973              |null |1    |3.124010000E+24 |
     * |77105880035     |137299000000 |5/20/2015 |6973              |null |1    |3.0931300000E+25|
     * |1000000000000000|-1           |12/11/2015|6973              |null |1    |3.1753100000E+25|
     * |62455802158     |-1           |1/8/2016  |6973              |null |1    |3.1846100000E+25|
     * |1000000000000000|-1           |5/22/2015 |6973              |null |2    |3.094490000E+24 |
     * +----------------+-------------+----------+------------------+-----+-----+----------------+
     *
     * 3. Some files have trans_id column and other have trans_key (refer to above dataframe snippets)
     *
     */
    val df = readFromCSV(spark, bucketName, ele.getName)

    var transactionFactsDF = df

    println(s"gs://$bucketName//landing/${ele.getName} read by Spark")

    /*
     * This method will parse trans_dt column to "MM/dd/yyyy" format (i.e, 7/24/2015 to 07/24/2015). This transformation
     * is necessary as Spark to_date method cannot parse the "MM/dd/yyyy" if the month has 1 digit.
     */
    val parseTransDt = (strDate: String) => {
      new SimpleDateFormat("MM/dd/yyyy").format(new SimpleDateFormat("MM/dd/yyyy").parse(strDate))
    }

    val parseTransDtUDF = udf(parseTransDt)

    /*
     * This code snippet will normalize the trans_dt format and extract the trans_year and trans_month values.
     */
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

    /**
     * As we have two sets of column names, each of the code snippet will work with normalizing trans_id & trans_key into one trans_key String format.
     */
    if (transactionFactsDF.columns.contains("trans_id")) {
      transactionFactsDF = transactionFactsDF.withColumn("trans_key", col("trans_id").cast(DecimalType(30, 0)).cast(StringType))
    } else {
      transactionFactsDF = transactionFactsDF.withColumn("trans_key", col("trans_key").cast(DecimalType(30, 0)).cast(StringType))
    }

    /**
     * This code snippet will fill the null values in sales and units columns with 0.0 and 0 respectively.
     */
    transactionFactsDF = transactionFactsDF
      .na.fill(0.0, Array("sales"))
      .na.fill(0, Array("units"))

    /*
     * This common method will write the dataframe into GCS in CSV format without any partitions.
     */
    writeToCSV(df, SaveMode.Overwrite, s"gs://$bucketName/archive/landing/transactions/${LocalDateTime.now()}/")

    println(s"gs://$bucketName/landing/${ele.getName} archived by Spark")

    transactionFactsDF
  }
}