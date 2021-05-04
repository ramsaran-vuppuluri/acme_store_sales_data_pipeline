package ca.acme.store.staging

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count}
import org.scalatest.FunSuite

class LocationsDataTest extends FunSuite {
  val spark = SparkSession.builder().master("local").appName("LocationsDataTest").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val locationDF = spark.read.option("inferSchema", "true").option("header", "true").csv("./src/test/resources/location.csv")

  test("printSchema") {
    println("Test name:::: locationDF Schema ::::")
    locationDF.printSchema()

    println(":::::::::::::::::::::::::")
  }

  test("showRecords") {
    println("Test name:::: locationDF 20 random records ::::")

    locationDF.show(20, false)

    println(":::::::::::::::::::::::::")
  }

  test("countRecords") {
    println("Test name:::: locationDF count records ::::")

    println(s"Location count ${locationDF.count}")

    locationDF.groupBy(col("province")).agg(count(col("province"))).show(20, false)

    println(":::::::::::::::::::::::::")
  }

  test("nullCheck") {
    println("Test name:::: locationDF null columns check ::::")

    val columns = locationDF.columns

    val nullColumns = columns.filter {
      column: String => {
        locationDF.where(col(column).isNull).count() > 0
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

  test("duplicateStoreNum") {
    println("Test name:::: locationDF check if there are any duplicate store_num values ::::")

    locationDF
      .groupBy(col("store_num"))
      .agg(count(col("store_num")).as("store_num_count"))
      .where(col("store_num_count") > 1)
      .show(10, false)

    println(":::::::::::::::::::::::::")
  }

  test("duplicateStoreLocationKey") {
    println("Test name:::: locationDF check if there are any duplicate store_location_key values ::::")

    locationDF
      .groupBy(col("store_location_key"))
      .agg(count(col("store_location_key")).as("store_location_key_count"))
      .where(col("store_location_key_count") > 1)
      .show(10, false)

    println(":::::::::::::::::::::::::")
  }
}
