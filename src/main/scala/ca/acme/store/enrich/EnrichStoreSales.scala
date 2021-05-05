package ca.acme.store.enrich

/**
 * @author Ram Saran Vuppuluri.
 *
 *         This object will read transactions data with products & locations data from staging region and persist the
 *         consolidated data to enrichment zone. Data is partitioned at province, year and month level.
 *
 *         All the joins are left joins to not loose any data.
 */

import ca.acme.store.generic.GcsIO
import ca.acme.store.schema.TransactionEnrichmentSchema
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object EnrichStoreSales extends GcsIO {
  /**
   * This method will join the transactions data with products & locations data from staging region.
   *
   * @param spark      Instance of spark session.
   * @param bucketName Bucket name in which data is processed in.
   */
  def apply(spark: SparkSession, bucketName: String): Unit = {
    /*
     * This method will read from parquet files and generate Spark DataFrame.
     */
    val locationDFIn = readFromParquet(spark, bucketName, "staging/location/")
    val productsDFIn = readFromParquet(spark, bucketName, "staging/product/")
    val transactionsDF = readFromParquet(spark, bucketName, "staging/transactions/")

    /*
     * Logger will capture the count pre and post join.
     */
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

    /*
     * +------------------+---------------+-------------+----------+-----+-----+--------------------------+----------+-----------+----+----+---------+----------------+--------+----------+
     * |store_location_key|product_key    |collector_key|trans_dt  |sales|units|trans_key                 |trans_year|trans_month|sku |upc |item_name|item_description|category|department|
     * +------------------+---------------+-------------+----------+-----+-----+--------------------------+----------+-----------+----+----+---------+----------------+--------+----------+
     * |9604              |999999999999513|-1           |2015-12-09|0.0  |1    |672879604118220151209919  |2015      |12         |null|null|null     |null            |null    |null      |
     * |9604              |999999999999513|-1           |2015-12-04|0.0  |1    |671159604118220151204726  |2015      |12         |null|null|null     |null            |null    |null      |
     * |9604              |999999999999513|-1           |2015-12-17|0.0  |1    |675929604118220151217940  |2015      |12         |null|null|null     |null            |null    |null      |
     * |9604              |999999999999513|-1           |2015-12-22|0.0  |1    |6779296041182201512221359 |2015      |12         |null|null|null     |null            |null    |null      |
     * |9604              |999999999999513|-1           |2015-12-10|0.0  |1    |6736096041182201512101438 |2015      |12         |null|null|null     |null            |null    |null      |
     * |1396              |999999999999513|-1           |2015-12-23|68.99|1    |62752513961182201512231159|2015      |12         |null|null|null     |null            |null    |null      |
     * |9604              |999999999999513|-1           |2015-12-10|0.0  |1    |6732796041182201512101011 |2015      |12         |null|null|null     |null            |null    |null      |
     * |9604              |999999999999513|-1           |2015-12-22|0.0  |1    |6779796041182201512221517 |2015      |12         |null|null|null     |null            |null    |null      |
     * |9604              |999999999999513|-1           |2015-12-29|8.9  |1    |679609604118220151229806  |2015      |12         |null|null|null     |null            |null    |null      |
     * |9604              |999999999999513|-1           |2015-12-19|13.76|1    |6767496041182201512191003 |2015      |12         |null|null|null     |null            |null    |null      |
     * +------------------+---------------+-------------+----------+-----+-----+--------------------------+----------+-----------+----+----+---------+----------------+--------+----------+
     */

    println(s"enrichDF count after joining transactionsDF with productsDF :: ${enrichDF.count}")

    enrichDF = enrichDF.join(locationDF,
      enrichDF("store_location_key") === locationDF("store_location_key_left"),
      "left").drop("store_location_key_left")

    /*
     * +------------------+---------------+-------------+----------+-----+-----+--------------------------+----------+-----------+----+----+---------+----------------+--------+----------+------+------------+-----------+--------+---------+--------+
     * |store_location_key|product_key    |collector_key|trans_dt  |sales|units|trans_key                 |trans_year|trans_month|sku |upc |item_name|item_description|category|department|region|city        |postal_code|banner  |store_num|province|
     * +------------------+---------------+-------------+----------+-----+-----+--------------------------+----------+-----------+----+----+---------+----------------+--------+----------+------+------------+-----------+--------+---------+--------+
     * |9604              |999999999999513|-1           |2015-12-09|0.0  |1    |672879604118220151209919  |2015      |12         |null|null|null     |null            |null    |null      |NA    |PETERBOROUGH|NA         |6e36648 |9604     |ONTARIO |
     * |9604              |999999999999513|-1           |2015-12-04|0.0  |1    |671159604118220151204726  |2015      |12         |null|null|null     |null            |null    |null      |NA    |PETERBOROUGH|NA         |6e36648 |9604     |ONTARIO |
     * |9604              |999999999999513|-1           |2015-12-17|0.0  |1    |675929604118220151217940  |2015      |12         |null|null|null     |null            |null    |null      |NA    |PETERBOROUGH|NA         |6e36648 |9604     |ONTARIO |
     * |9604              |999999999999513|-1           |2015-12-22|0.0  |1    |6779296041182201512221359 |2015      |12         |null|null|null     |null            |null    |null      |NA    |PETERBOROUGH|NA         |6e36648 |9604     |ONTARIO |
     * |9604              |999999999999513|-1           |2015-12-10|0.0  |1    |6736096041182201512101438 |2015      |12         |null|null|null     |null            |null    |null      |NA    |PETERBOROUGH|NA         |6e36648 |9604     |ONTARIO |
     * |1396              |999999999999513|-1           |2015-12-23|68.99|1    |62752513961182201512231159|2015      |12         |null|null|null     |null            |null    |null      |NA    |BARRIE      |NA         |dc35b695|1396     |ONTARIO |
     * |9604              |999999999999513|-1           |2015-12-10|0.0  |1    |6732796041182201512101011 |2015      |12         |null|null|null     |null            |null    |null      |NA    |PETERBOROUGH|NA         |6e36648 |9604     |ONTARIO |
     * |9604              |999999999999513|-1           |2015-12-22|0.0  |1    |6779796041182201512221517 |2015      |12         |null|null|null     |null            |null    |null      |NA    |PETERBOROUGH|NA         |6e36648 |9604     |ONTARIO |
     * |9604              |999999999999513|-1           |2015-12-29|8.9  |1    |679609604118220151229806  |2015      |12         |null|null|null     |null            |null    |null      |NA    |PETERBOROUGH|NA         |6e36648 |9604     |ONTARIO |
     * |9604              |999999999999513|-1           |2015-12-19|13.76|1    |6767496041182201512191003 |2015      |12         |null|null|null     |null            |null    |null      |NA    |PETERBOROUGH|NA         |6e36648 |9604     |ONTARIO |
     * +------------------+---------------+-------------+----------+-----+-----+--------------------------+----------+-----------+----+----+---------+----------------+--------+----------+------+------------+-----------+--------+---------+--------+
     */

    println(s"enrichDF count after joining enrichDF with locationDF :: ${enrichDF.count}")

    val columns = TransactionEnrichmentSchema.schema.map {
      ele => ele.name
    }

    enrichDF = enrichDF.selectExpr(columns: _*)

    /*
     * +-------------+----------+-----+-----+--------------------------+----------+-----------+---------------+----+----+---------+----------------+----------+--------+------------------+------+--------+------------+-----------+--------+---------+
     * |collector_key|trans_dt  |sales|units|trans_key                 |trans_year|trans_month|product_key    |sku |upc |item_name|item_description|department|category|store_location_key|region|province|city        |postal_code|banner  |store_num|
     * +-------------+----------+-----+-----+--------------------------+----------+-----------+---------------+----+----+---------+----------------+----------+--------+------------------+------+--------+------------+-----------+--------+---------+
     * |-1           |2015-12-09|0.0  |1    |672879604118220151209919  |2015      |12         |999999999999513|null|null|null     |null            |null      |null    |9604              |NA    |ONTARIO |PETERBOROUGH|NA         |6e36648 |9604     |
     * |-1           |2015-12-04|0.0  |1    |671159604118220151204726  |2015      |12         |999999999999513|null|null|null     |null            |null      |null    |9604              |NA    |ONTARIO |PETERBOROUGH|NA         |6e36648 |9604     |
     * |-1           |2015-12-17|0.0  |1    |675929604118220151217940  |2015      |12         |999999999999513|null|null|null     |null            |null      |null    |9604              |NA    |ONTARIO |PETERBOROUGH|NA         |6e36648 |9604     |
     * |-1           |2015-12-22|0.0  |1    |6779296041182201512221359 |2015      |12         |999999999999513|null|null|null     |null            |null      |null    |9604              |NA    |ONTARIO |PETERBOROUGH|NA         |6e36648 |9604     |
     * |-1           |2015-12-10|0.0  |1    |6736096041182201512101438 |2015      |12         |999999999999513|null|null|null     |null            |null      |null    |9604              |NA    |ONTARIO |PETERBOROUGH|NA         |6e36648 |9604     |
     * |-1           |2015-12-23|68.99|1    |62752513961182201512231159|2015      |12         |999999999999513|null|null|null     |null            |null      |null    |1396              |NA    |ONTARIO |BARRIE      |NA         |dc35b695|1396     |
     * |-1           |2015-12-10|0.0  |1    |6732796041182201512101011 |2015      |12         |999999999999513|null|null|null     |null            |null      |null    |9604              |NA    |ONTARIO |PETERBOROUGH|NA         |6e36648 |9604     |
     * |-1           |2015-12-22|0.0  |1    |6779796041182201512221517 |2015      |12         |999999999999513|null|null|null     |null            |null      |null    |9604              |NA    |ONTARIO |PETERBOROUGH|NA         |6e36648 |9604     |
     * |-1           |2015-12-29|8.9  |1    |679609604118220151229806  |2015      |12         |999999999999513|null|null|null     |null            |null      |null    |9604              |NA    |ONTARIO |PETERBOROUGH|NA         |6e36648 |9604     |
     * |-1           |2015-12-19|13.76|1    |6767496041182201512191003 |2015      |12         |999999999999513|null|null|null     |null            |null      |null    |9604              |NA    |ONTARIO |PETERBOROUGH|NA         |6e36648 |9604     |
     * +-------------+----------+-----+-----+--------------------------+----------+-----------+---------------+----+----+---------+----------------+----------+--------+------------------+------+--------+------------+-----------+--------+---------+
     */

    println(s"enrichDF write count :: ${enrichDF.count}")


    writeToParquet(enrichDF, SaveMode.Append, TransactionEnrichmentSchema.partitionColumns, s"gs://$bucketName/enrichment/transactions/")
  }
}
