package ca.acme.store.staging

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count}
import org.scalatest.FunSuite

class ProductsDataTest extends FunSuite {
  val spark = SparkSession.builder().master("local").appName("ProductsDataTest").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val productsDF = spark.read.option("inferSchema", "true").option("header", "true").csv("./src/test/resources/product.csv")

  test("printSchema") {
    println("Test name:::: productsDF Schema ::::")

    productsDF.printSchema()

    println(":::::::::::::::::::::::::")
  }

  test("showRecords") {
    println("Test name:::: productsDF 20 random records ::::")

    productsDF.show(20, false)

    println(":::::::::::::::::::::::::")
  }

  test("countRecords") {
    println("Test name:::: productsDF count records ::::")

    println(s"Location count ${productsDF.count}")

    println(":::::::::::::::::::::::::")
  }

  test("nullCheck") {
    println("Test name:::: productsDF null columns check ::::")

    val columns = productsDF.columns

    val nullColumns = columns.filter {
      column: String => {
        productsDF.where(col(column).isNull).count() > 0
      }
    }

    if (nullColumns == null || nullColumns.size == 0) {
      println("No null columns")
    } else {
      nullColumns.foreach {
        column => {
          println(s"$column contains null values.")
        }
      }
    }

    println(":::::::::::::::::::::::::")
  }

  test("duplicateProductkey") {
    println("Test name:::: productsDF check if there are any duplicate product_key values ::::")

    productsDF
      .groupBy(col("product_key"))
      .agg(count(col("product_key")).as("product_key_count"))
      .where(col("product_key_count") > 1)
      .show(20, false)

    println(":::::::::::::::::::::::::")
  }

  test("duplicateUpc") {
    println("Test name:::: productsDF check if there are any duplicate upc values ::::")

    productsDF
      .groupBy(col("upc"))
      .agg(count(col("upc")).as("upc_count"))
      .where(col("upc_count") > 1)
      .show(20, false)

    println(":::::::::::::::::::::::::")
  }

  test("productKey"){
    productsDF.where(col("product_key") === "999999999999513").show(30, false)
  }
}