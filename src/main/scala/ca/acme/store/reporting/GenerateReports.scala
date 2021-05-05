package ca.acme.store.reporting

/**
 * @author Ram Saran Vuppuluri
 *
 *         This object will read consolidated data from enrichment zone and generate analysis reports as standard outputs.
 *         For visualization, aggregated data is stored as csv's.
 */

import ca.acme.store.generic.GcsIO
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col, lit, rank, sum, when, countDistinct}
import org.apache.spark.sql.types.{DecimalType, StringType}

object GenerateReports extends GcsIO {
  /**
   * This method will consolidated data from enrichment zone and generate analysis reports as standard outputs.
   *
   * @param spark      Instance of spark session.
   * @param bucketName Bucket name in which data is processed in.
   */
  def apply(spark: SparkSession, bucketName: String): Unit = {
    val enrichDF = readFromParquet(spark, bucketName, "enrichment/transactions/")

    salesByProvince(enrichDF, bucketName)

    salesByStore(enrichDF, bucketName)

    salesByCategory(enrichDF, bucketName)

    topNPerformingStoresByProvince(enrichDF, bucketName)

    topNProductCategoriesByDepartment(enrichDF, bucketName)

    salesMadeByLoyaltyVsNonLoyalty(enrichDF, bucketName)

    salesAndUnitsByYear(enrichDF, bucketName)

    salesAndUnitsByLoyalty(enrichDF, bucketName)
  }

  /**
   * This method will generate the metrics for loyalty and non-loyalty members.
   *
   * @param enrichDFIn Enrichment dataframe.
   * @param bucketName Bucket name.
   */
  private def salesAndUnitsByLoyalty(enrichDFIn: DataFrame, bucketName: String): Unit = {
    var enrichDF = enrichDFIn
      .withColumn("is_member", when((col("collector_key").isNull || col("collector_key") === lit("-1")), lit("No")).otherwise(lit("Yes")))

    enrichDF = enrichDF.groupBy("is_member")
      .agg(
        sum("sales").cast(DecimalType(28, 3)).as("total_sales"),
        sum("units").as("total_units"),
        countDistinct("trans_key").as("count_distinct_trans_key"),
        countDistinct("collector_key").as("count_distinct_collector_key")
      )
      .coalesce(1)

    /*
     * +---------+-----------+-----------+------------------------+----------------------------+
     * |is_member|total_sales|total_units|count_distinct_trans_key|count_distinct_collector_key|
     * +---------+-----------+-----------+------------------------+----------------------------+
     * |No       |2628913.670|126209     |31080                   |1                           |
     * |Yes      |1631568.310|102723     |39738                   |7678                        |
     * +---------+-----------+-----------+------------------------+----------------------------+
     */

    writeToCSV(enrichDF, SaveMode.Overwrite, s"gs://$bucketName/reporting/salesAndUnitsByLoyalty/")
  }

  /**
   * This method will generate the year over year metrics.
   *
   * @param enrichDF   Enrichment dataframe.
   * @param bucketName Bucket name.
   */
  private def salesAndUnitsByYear(enrichDF: DataFrame, bucketName: String): Unit = {
    val df = enrichDF.groupBy("trans_year")
      .agg(
        sum("sales").cast(DecimalType(28, 3)).as("total_sales"),
        sum("units").as("total_units"),
        countDistinct("trans_key").as("count_distinct_trans_key"),
        countDistinct("collector_key").as("count_distinct_collector_key")
      )
      .coalesce(1)

    /*
     * +----------+-----------+-----------+------------------------+----------------------------+
     * |trans_year|total_sales|total_units|count_distinct_trans_key|count_distinct_collector_key|
     * +----------+-----------+-----------+------------------------+----------------------------+
     * |2015      |3954171.770|211937     |64944                   |7368                        |
     * |2016      |306310.210 |16995      |5996                    |765                         |
     * +----------+-----------+-----------+------------------------+----------------------------+
     */

    writeToCSV(df, SaveMode.Overwrite, s"gs://$bucketName/reporting/salesByYear/")
  }

  /**
   * This method will generate the top n Prodcut Categories in each department.  By default value of n is 10.
   *
   * @param enrichDF   Enrichment dataframe.
   * @param bucketName Bucket name.
   * @param n          Top n.
   */
  private def topNProductCategoriesByDepartment(enrichDF: DataFrame, bucketName: String, n: Int = 10): Unit = {
    var df = enrichDF
      .groupBy("department", "category")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))

    val windowSpec = Window
      .partitionBy("department")
      .orderBy(col("total_sales").desc)

    df = df.withColumn("category_rank_in_department", rank.over(windowSpec))
      .where(col("category_rank_in_department") <= lit(n))
      .selectExpr("category_rank_in_department",
        "department",
        "category",
        "total_sales")
      .coalesce(1)

    /*
     * +---------------------------+----------+--------+-----------+
     * |category_rank_in_department|department|category|total_sales|
     * +---------------------------+----------+--------+-----------+
     * |1                          |null      |null    |3676371.920|
     * |1                          |34a2a7e0  |1578f747|0.000      |
     * |1                          |5bffa719  |a4d52407|6541.740   |
     * |2                          |5bffa719  |6c504249|2555.540   |
     * |3                          |5bffa719  |fa3a1bd8|2150.030   |
     * |4                          |5bffa719  |a121fb78|1643.760   |
     * |5                          |5bffa719  |3ae24ac2|985.090    |
     * |6                          |5bffa719  |999b3a55|423.080    |
     * |7                          |5bffa719  |8f610228|365.840    |
     * |8                          |5bffa719  |108c2838|338.030    |
     * |9                          |5bffa719  |2dc2366a|158.450    |
     * |10                         |5bffa719  |3f1695bd|56.770     |
     * |1                          |7569cb40  |cef3760b|38550.160  |
     * |1                          |a461091   |687ed9e3|20948.800  |
     * |1                          |24d07cc8  |50c418ce|19.080     |
     * |1                          |435ca98   |e0b38f5b|18745.720  |
     * |2                          |435ca98   |21b19a94|6474.860   |
     * |3                          |435ca98   |640d751d|6240.200   |
     * |4                          |435ca98   |2836e915|1913.390   |
     * |5                          |435ca98   |e8eeb80f|312.880    |
     * |1                          |b947a4a9  |382cf3a |13475.830  |
     * |2                          |b947a4a9  |3b380e17|6169.610   |
     * |3                          |b947a4a9  |8b4f9982|6055.670   |
     * |4                          |b947a4a9  |29ecadc0|2232.830   |
     * |5                          |b947a4a9  |6761045 |1861.410   |
     * |6                          |b947a4a9  |6023eeb7|1418.280   |
     * |7                          |b947a4a9  |f2672c8c|1015.030   |
     * |8                          |b947a4a9  |9db5a1ff|365.770    |
     * |1                          |1a34cbb9  |fe148072|39543.470  |
     * |2                          |1a34cbb9  |ffcec4a7|34951.150  |
     * |3                          |1a34cbb9  |e49d14f1|26665.080  |
     * |4                          |1a34cbb9  |11566ced|18364.200  |
     * |5                          |1a34cbb9  |e934fcda|17827.150  |
     * |6                          |1a34cbb9  |a97ccc2c|14904.510  |
     * |7                          |1a34cbb9  |7703921f|14839.620  |
     * |8                          |1a34cbb9  |d18e3df7|12050.570  |
     * |9                          |1a34cbb9  |d47ae288|8135.600   |
     * |10                         |1a34cbb9  |7b703ee1|7948.660   |
     * |1                          |89d0c9d1  |511e5c1b|78.320     |
     * |1                          |3ad17de9  |c280daf5|3709.050   |
     * |2                          |3ad17de9  |43caffcc|1621.920   |
     * |3                          |3ad17de9  |caddeda1|775.940    |
     * |4                          |3ad17de9  |ecc023a5|37.380     |
     * |5                          |3ad17de9  |14a2b392|-115.700   |
     * |1                          |651b1068  |7aaa7a34|6951.720   |
     * |2                          |651b1068  |ed1a6770|5358.110   |
     * |3                          |651b1068  |e5475024|5325.440   |
     * |4                          |651b1068  |c35a9e20|3434.380   |
     * |5                          |651b1068  |b7dbc305|3221.130   |
     * |6                          |651b1068  |5cb7b430|2698.320   |
     * +---------------------------+----------+--------+-----------+
     * only showing top 50 rows
     */

    writeToCSV(df, SaveMode.Overwrite, s"gs://$bucketName/reporting/topNProductCategoriesByDepartment/")
  }

  /**
   * This method wiil generate the purchase patterns of loyalty vs non-loyalty customers by province.
   *
   * @param enrichDFIn Enrichment dataframe.
   * @param bucketName Bucket name.
   */
  private def salesMadeByLoyaltyVsNonLoyalty(enrichDFIn: DataFrame, bucketName: String): Unit = {
    val enrichDF = enrichDFIn
      .withColumn("is_member", when((col("collector_key").isNull || col("collector_key") === lit("-1")), lit("No")).otherwise(lit("Yes")))

    val enrichByProvinceDF = enrichDF
      .groupBy("province", "is_member")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))
      .orderBy("province", "is_member")
      .coalesce(1)

    /*
     * +----------------+---------+-----------+
     * |province        |is_member|total_sales|
     * +----------------+---------+-----------+
     * |ALBERTA         |No       |1163292.130|
     * |ALBERTA         |Yes      |349230.240 |
     * |BRITISH COLUMBIA|No       |113437.940 |
     * |BRITISH COLUMBIA|Yes      |162098.510 |
     * |MANITOBA        |No       |95836.340  |
     * |MANITOBA        |Yes      |138599.260 |
     * |ONTARIO         |No       |1240442.000|
     * |ONTARIO         |Yes      |942834.400 |
     * |SASKATCHEWAN    |No       |15905.260  |
     * |SASKATCHEWAN    |Yes      |38805.900  |
     * +----------------+---------+-----------+
     */

    writeToCSV(enrichByProvinceDF, SaveMode.Overwrite, s"gs://$bucketName/reporting/salesMadeByLoyaltyVsNonLoyaltyByProvince/")

    val enrichByMembershipDF = enrichDF
      .groupBy("is_member")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))
      .orderBy("is_member")
      .coalesce(1)

    /*
     * +---------+-----------+
     * |is_member|total_sales|
     * +---------+-----------+
     * |No       |2628913.670|
     * |Yes      |1631568.310|
     * +---------+-----------+
     */

    writeToCSV(enrichByMembershipDF, SaveMode.Overwrite, s"gs://$bucketName/reporting/salesMadeByLoyaltyVsNonLoyalty/")
  }

  /**
   * This method will generate the top n stores in each Province.  By default value of n is 5.
   *
   * @param enrichDF   Enrichment dataframe.
   * @param bucketName Bucket name.
   * @param n          Top n.
   */
  private def topNPerformingStoresByProvince(enrichDF: DataFrame, bucketName: String, n: Int = 5): Unit = {
    println(s"Top $n performing Stores by Province.")
    var df = enrichDF
      .groupBy("province", "store_num")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))

    val windowSpec = Window
      .partitionBy("province")
      .orderBy(col("total_sales").desc)

    df = df.withColumn("store_rank_in_provice", rank.over(windowSpec))
      .withColumn("avg_store_total_sales_in_province", avg("total_sales").over(Window.partitionBy("province")))
      .withColumn("avg_store_total_sales_in_province", col("avg_store_total_sales_in_province").cast(DecimalType(28, 3)))
      .where(col("store_rank_in_provice") <= lit(n))
      .selectExpr("store_rank_in_provice",
        "province",
        "store_num",
        "total_sales",
        "avg_store_total_sales_in_province")
      .coalesce(1)

    /*
     * Answers following questions:
     * 1. Which Provinces and Stores are performing well and how much the top stores in each province performing in
     * comparison with average store in the province.
     * 2. Top 5 Store in the province.
     *
     * +---------------------+----------------+---------+-----------+---------------------------------+
     * |store_rank_in_provice|province        |store_num|total_sales|avg_store_total_sales_in_province|
     * +---------------------+----------------+---------+-----------+---------------------------------+
     * |1                    |BRITISH COLUMBIA|7167     |113885.400 |45922.742                        |
     * |2                    |BRITISH COLUMBIA|7104     |101377.430 |45922.742                        |
     * |3                    |BRITISH COLUMBIA|7125     |28011.520  |45922.742                        |
     * |4                    |BRITISH COLUMBIA|7194     |17304.700  |45922.742                        |
     * |5                    |BRITISH COLUMBIA|7175     |14096.530  |45922.742                        |
     * |1                    |SASKATCHEWAN    |7317     |46439.700  |18237.053                        |
     * |2                    |SASKATCHEWAN    |7309     |4487.250   |18237.053                        |
     * |3                    |SASKATCHEWAN    |7313     |3784.210   |18237.053                        |
     * |1                    |ALBERTA         |9807     |1030351.770|116347.875                       |
     * |2                    |ALBERTA         |7296     |385435.050 |116347.875                       |
     * |3                    |ALBERTA         |9802     |55884.250  |116347.875                       |
     * |4                    |ALBERTA         |7247     |17828.780  |116347.875                       |
     * |5                    |ALBERTA         |7226     |9994.970   |116347.875                       |
     * |1                    |MANITOBA        |4823     |234156.570 |78145.200                        |
     * |2                    |MANITOBA        |4861     |257.690    |78145.200                        |
     * |3                    |MANITOBA        |7403     |21.340     |78145.200                        |
     * |1                    |ONTARIO         |8142     |872304.400 |99239.836                        |
     * |2                    |ONTARIO         |6973     |600223.000 |99239.836                        |
     * |3                    |ONTARIO         |1396     |467581.280 |99239.836                        |
     * |4                    |ONTARIO         |1891     |76338.690  |99239.836                        |
     * |5                    |ONTARIO         |6979     |50069.720  |99239.836                        |
     * +---------------------+----------------+---------+-----------+---------------------------------+
     */

    writeToCSV(df, SaveMode.Overwrite, s"gs://$bucketName/reporting/topNPerformingStoresByProvince/")
  }

  /**
   * This method will generate total amount of sales by category.
   *
   * @param enrichDF   Enrichment dataframe.
   * @param bucketName Bucket name.
   */
  private def salesByCategory(enrichDF: DataFrame, bucketName: String): Unit = {
    println("Total amount of Sales by Category.")

    val df = enrichDF
      .groupBy("category")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))
      .orderBy(col("total_sales").desc)
      .coalesce(1)

    /*
     * null category is because there are multiple transactions with no matching product key.
     *
     * +--------+-----------+
     * |category|total_sales|
     * +--------+-----------+
     * |null    |3676371.920|
     * |fe148072|39543.470  |
     * |cef3760b|38550.160  |
     * |ffcec4a7|34951.150  |
     * |e49d14f1|26665.080  |
     * |d5a0a65d|22607.800  |
     * |65d731c8|21481.070  |
     * |687ed9e3|20948.800  |
     * |e0b38f5b|18745.720  |
     * |11566ced|18364.200  |
     * |e934fcda|17827.150  |
     * |5530c7b1|17144.060  |
     * |190b5bb9|15984.200  |
     * |a97ccc2c|14904.510  |
     * |7703921f|14839.620  |
     * |2588bef |14669.970  |
     * |382cf3a |13475.830  |
     * |d18e3df7|12050.570  |
     * |206bde41|11038.310  |
     * |1d25c013|10518.230  |
     * +--------+-----------+
     */
    df.show(20, false)

    writeToCSV(df, SaveMode.Overwrite, s"gs://$bucketName/reporting/salesByCategory/")
  }

  /**
   * This method will generate total amount of sales by store.
   *
   * @param enrichDF   Enrichment dataframe.
   * @param bucketName Bucket name.
   */
  private def salesByStore(enrichDF: DataFrame, bucketName: String): Unit = {
    println("Total amount of Sales by Store.")

    val df = enrichDF
      .groupBy("store_num")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))
      .orderBy(col("total_sales").desc)
      .coalesce(1)

    /*
     * +---------+-----------+
     * |store_num|total_sales|
     * +---------+-----------+
     * |9807     |1030351.770|
     * |8142     |872304.400 |
     * |6973     |600223.000 |
     * |1396     |467581.280 |
     * |7296     |385435.050 |
     * |4823     |234156.570 |
     * |7167     |113885.400 |
     * |7104     |101377.430 |
     * |1891     |76338.690  |
     * |9802     |55884.250  |
     * |6979     |50069.720  |
     * |7317     |46439.700  |
     * |9604     |42964.450  |
     * |7125     |28011.520  |
     * |6905     |19713.240  |
     * |7247     |17828.780  |
     * |7194     |17304.700  |
     * |2428     |14467.610  |
     * |7175     |14096.530  |
     * |6946     |12490.780  |
     * +---------+-----------+
     * only showing top 20 rows
     */
    df.show(20, false)

    writeToCSV(df, SaveMode.Overwrite, s"gs://$bucketName/reporting/salesByStore/")
  }

  /**
   * This method will generate total amount of sales by province.
   *
   * @param enrichDF   Enrichment dataframe.
   * @param bucketName Bucket name.
   */
  private def salesByProvince(enrichDF: DataFrame, bucketName: String): Unit = {
    println("Total amount of Sales by Province.")
    val df = enrichDF
      .groupBy("province")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))
      .coalesce(1)

    /*
     * +----------------+-----------+
     * |province        |total_sales|
     * +----------------+-----------+
     * |BRITISH COLUMBIA|275536.450 |
     * |SASKATCHEWAN    |54711.160  |
     * |ALBERTA         |1512522.370|
     * |MANITOBA        |234435.600 |
     * |ONTARIO         |2183276.400|
     * +----------------+-----------+
     */
    df.show(20, false)

    writeToCSV(df, SaveMode.Overwrite, s"gs://$bucketName/reporting/salesByProvince/")
  }
}
