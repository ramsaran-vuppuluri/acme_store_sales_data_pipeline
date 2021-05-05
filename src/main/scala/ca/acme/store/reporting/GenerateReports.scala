package ca.acme.store.reporting

import ca.acme.store.generic.GcsIO
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col, lit, rank, sum, when, countDistinct}
import org.apache.spark.sql.types.{DecimalType, StringType}

object GenerateReports extends GcsIO {
  def apply(spark: SparkSession, bucketName: String): Unit = {
    val enrichDF = readFromParquet(spark, bucketName, "enrichment/transactions/")

    salesByProvince(enrichDF, bucketName)

    salesByStore(enrichDF, bucketName)

    salesByCategory(enrichDF, bucketName)

    topNPerformingStoresByProvince(enrichDF, bucketName)

    topNProductCategoriesByDepartment(enrichDF, bucketName)

    salesMadeByLoyaltyVsNonLoyalty(enrichDF, bucketName)

    salesAndUnitsByYear(enrichDF, bucketName)
  }

  private def salesAndUnitsByYear(enrichDF: DataFrame, bucketName: String): Unit = {
    val df = enrichDF.groupBy("trans_year")
      .agg(
        sum("sales").cast(DecimalType(28, 3)).as("total_sales"),
        sum("units").as("total_units"),
        countDistinct("trans_key").as("count_distinct_trans_key"),
        countDistinct("collector_key").as("count_distinct_collector_key")
      )
      .coalesce(1)

    writeToCSV(df, SaveMode.Overwrite, s"gs://$bucketName/reporting/salesByYear/")
  }

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

    df.show(50, false)

    writeToCSV(df, SaveMode.Overwrite, s"gs://$bucketName/reporting/topNProductCategoriesByDepartment/")
  }

  private def salesMadeByLoyaltyVsNonLoyalty(enrichDFIn: DataFrame, bucketName: String): Unit = {
    var enrichDF = enrichDFIn
      .withColumn("is_member", when((col("collector_key").isNull || col("collector_key") === lit("-1")), lit("No")).otherwise(lit("Yes")))

    enrichDF = enrichDF
      .groupBy("province", "is_member")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))
      .orderBy("province", "is_member")
      .coalesce(1)

    enrichDF.show(20, false)

    writeToCSV(enrichDF, SaveMode.Overwrite, s"gs://$bucketName/reporting/salesMadeByLoyaltyVsNonLoyalty/")
  }

  private def topNPerformingStoresByProvince(enrichDF: DataFrame, bucketName: String, n: Int = 5): Unit = {
    println(s"Top $n performing Stores by Province.")

    val answer =
      s"""
         |Answers following questions:
         |1. Which Provinces and Stores are performing well and how much the top stores in each province performing in
         |comparison with average store in the province.
         |2. Top 5 Store in the province.
         |""".stripMargin

    println(answer)

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

    df.show(50, false)

    writeToCSV(df, SaveMode.Overwrite, s"gs://$bucketName/reporting/topNPerformingStoresByProvince/")
  }

  private def salesByCategory(enrichDF: DataFrame, bucketName: String): Unit = {
    println("Total amount of Sales by Category.")

    val df = enrichDF
      .groupBy("category")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))
      .orderBy(col("total_sales").desc)
      .coalesce(1)

    df.show(20, false)

    writeToCSV(df, SaveMode.Overwrite, s"gs://$bucketName/reporting/salesByCategory/")
  }

  private def salesByStore(enrichDF: DataFrame, bucketName: String): Unit = {
    println("Total amount of Sales by Store.")

    val df = enrichDF
      .groupBy("store_num")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))
      .orderBy(col("total_sales").desc)
      .coalesce(1)

    df.show(20, false)

    writeToCSV(df, SaveMode.Overwrite, s"gs://$bucketName/reporting/salesByStore/")
  }


  private def salesByProvince(enrichDF: DataFrame, bucketName: String): Unit = {
    println("Total amount of Sales by Province.")
    val df = enrichDF
      .groupBy("province")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))
      .coalesce(1)

    df.show(20, false)

    writeToCSV(df, SaveMode.Overwrite, s"gs://$bucketName/reporting/salesByProvince/")
  }
}
