package ca.acme.store.reporting

import ca.acme.store.generic.GcsIO
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, col, lit, rank, sum, when}
import org.apache.spark.sql.types.DecimalType

object GenerateReports extends GcsIO {
  def apply(spark: SparkSession, bucketName: String): Unit = {
    val enrichDF = readFromParquet(spark, bucketName, "enrichment/transactions/")

    salesByProvince(enrichDF)

    salesByStore(enrichDF)

    salesByCategory(enrichDF)

    topNPerformingStoresByProvince(enrichDF)

    topNProductCategoriesByDepartment(enrichDF)

    salesMadeByLoyaltyVsNonLoyalty(enrichDF)
  }

  private def topNProductCategoriesByDepartment(enrichDF: DataFrame, n: Int = 10): Unit = {
    val df = enrichDF
      .groupBy("department", "category")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))

    val windowSpec = Window
      .partitionBy("department")
      .orderBy(col("total_sales").desc)

    df.withColumn("category_rank_in_department", rank.over(windowSpec))
      .where(col("category_rank_in_department") <= lit(n))
      .selectExpr("category_rank_in_department",
        "department",
        "category",
        "total_sales")
      .show(50, false)
  }

  private def salesMadeByLoyaltyVsNonLoyalty(enrichDFIn: DataFrame): Unit = {
    val enrichDF = enrichDFIn
      .withColumn("is_member", when((col("collector_key").isNull || col("collector_key") === lit("-1")), lit("No")).otherwise(lit("Yes")))

    enrichDF
      .groupBy("province", "is_member")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))
      .orderBy("province", "is_member")
      .show(20, false)
  }

  private def topNPerformingStoresByProvince(enrichDF: DataFrame, n: Int = 5): Unit = {
    println(s"Top $n performing Stores by Province.")

    val answer =
      s"""
         |Answers following questions:
         |1. Which Provinces and Stores are performing well and how much the top stores in each province performing in
         |comparison with average store in the province.
         |2. Top 5 Store in the province.
         |""".stripMargin

    val df = enrichDF
      .groupBy("province", "store_num")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))

    val windowSpec = Window
      .partitionBy("province")
      .orderBy(col("total_sales").desc)

    df.withColumn("store_rank_in_provice", rank.over(windowSpec))
      .withColumn("avg_store_total_sales_in_province", avg("total_sales").over(Window.partitionBy("province")))
      .withColumn("avg_store_total_sales_in_province", col("avg_store_total_sales_in_province").cast(DecimalType(28, 3)))
      .where(col("store_rank_in_provice") <= lit(n))
      .selectExpr("store_rank_in_provice",
        "province",
        "store_num",
        "total_sales",
        "avg_store_total_sales_in_province")
      .show(50, false)
  }

  private def salesByCategory(enrichDF: DataFrame): Unit = {
    println("Total amount of Sales by Category.")

    enrichDF
      .groupBy("category")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))
      .orderBy(col("total_sales").desc)
      .show(20, false)
  }

  private def salesByStore(enrichDF: DataFrame): Unit = {
    println("Total amount of Sales by Store.")

    enrichDF
      .groupBy("store_num")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))
      .orderBy(col("total_sales").desc)
      .show(20, false)
  }


  private def salesByProvince(enrichDF: DataFrame): Unit = {
    println("Total amount of Sales by Province.")
    val df = enrichDF
      .groupBy("province")
      .agg(sum("sales").cast(DecimalType(28, 3)).as("total_sales"))

    df.show(20, false)
  }
}
