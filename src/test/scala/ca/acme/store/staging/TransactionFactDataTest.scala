package ca.acme.store.staging

import java.io.File
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter

import ca.acme.store.schema.TransactionStagingSchema
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, count, lit, max, min, month, to_date, when, year, countDistinct}
import org.apache.spark.sql.types.{DateType, DecimalType, DoubleType, IntegerType, StringType}
import org.scalatest.FunSuite

class TransactionFactDataTest extends FunSuite {
  val spark = SparkSession.builder().master("local").appName("TransactionFactDataTest").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val transactionFiles = new File("./src/test/resources/").listFiles().filter(_.getName.startsWith("trans_fact_"))

  val transFactDF = spark.read.option("inferSchema", "true").option("header", "true").csv("./src/test/resources/trans_fact_*.csv")

  //val transFactDF = spark.read.schema(TransactionLandingSchema.schema).option("header", "true").format("csv").load("./src/test/resources/trans_fact_4.csv")

  test("listTransactionFiles") {
    transactionFiles.foreach(println)
  }

  test("printSchema") {
    println("Test name:::: transFactDF Schema ::::")

    transFactDF.printSchema()

    println(":::::::::::::::::::::::::")
  }

  test("showRecords") {
    println("Test name:::: transFactDF 20 random records ::::")

    transFactDF.show(20, false)

    println(":::::::::::::::::::::::::")
  }

  test("countRecords") {
    println("Test name:::: transFactDF count records ::::")

    println(s"Location count ${transFactDF.count}")

    println(":::::::::::::::::::::::::")
  }

  def nullCheck(transFactDF: DataFrame): Unit = {
    val columns = transFactDF.columns

    val nullColumns = columns.filter {
      column: String => {
        transFactDF.where(col(column).isNull).count() > 0
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
  }

  test("nullCheck") {
    println("Test name:::: transFactDF null columns check ::::")

    nullCheck(transFactDF)

    println(":::::::::::::::::::::::::")
  }

  test("tranDtRange") {
    println("Test name:::: productsDF transaction date range ::::")

    transFactDF
      .withColumn("trans_dt", col("trans_dt").cast(DateType))
      .select(min(col("trans_dt")).as("min_trans_dt"), max(col("trans_dt")).as("max_trans_dt"))
      .show(10, false)

    println(":::::::::::::::::::::::::")
  }

  test("tranDtCount") {
    transFactDF
      .withColumn("trans_dt", col("trans_dt").cast(DateType))
      .groupBy(col("trans_dt"))
      .agg(count(col("trans_dt")).as("count_trans_dt"))
      .orderBy(col("trans_dt"))
      .show(100, false)
  }

  test("schemaChanges") {
    val transactionFactsDF = transFactDF
      .withColumn("store_location_key", col("store_location_key").cast(StringType))
      .withColumn("product_key", col("product_key").cast(StringType))
      .withColumn("collector_key", col("collector_key").cast(StringType))
      .withColumn("trans_dt", col("trans_dt").cast(DateType))
      .withColumn("sales", col("sales").cast(DoubleType))
      .withColumn("units", col("units").cast(IntegerType))
      .withColumn("trans_key", col("trans_key").cast(StringType))

    transactionFactsDF.printSchema()

    transactionFactsDF.show(50, false)
  }

  test("transFactIteration") {
    transactionFiles.foreach {
      fileName: File => {
        println(s"File name ${fileName.getName}")

        val df = spark.read.option("inferSchema", "true").option("header", "true").csv(fileName.getAbsolutePath)

        df.show(10, false)

        val stagingColumns = TransactionStagingSchema.schema.map {
          ele => ele.name
        }

        if (df.columns.contains("trans_id")) {
          val df1 = df.withColumn("trans_dt_1", col("trans_dt").cast(DateType))
            .withColumn("trans_dt", when(col("trans_dt_1").isNull, to_date(col("trans_dt"), "MM/dd/yyyy")).otherwise(col("trans_dt_1")))
            .withColumn("trans_year", year(col("trans_dt")))
            .withColumn("trans_month", month(col("trans_dt")))
            .withColumn("trans_key", col("trans_id").cast(DecimalType(30, 0)).cast(StringType))
            .selectExpr(stagingColumns: _*)
            .na.fill(0.0, Array("sales"))
            .na.fill(0, Array("units"))

          println(df1.cache().count())

          nullCheck(df1)

        } else {
          val df1 = df.withColumn("trans_dt_1", col("trans_dt").cast(DateType))
            .withColumn("trans_dt", when(col("trans_dt_1").isNull, to_date(col("trans_dt"), "MM/dd/yyyy")).otherwise(col("trans_dt_1")))
            .withColumn("trans_year", year(col("trans_dt")))
            .withColumn("trans_month", month(col("trans_dt")))
            .withColumn("trans_key", col("trans_key").cast(DecimalType(30, 0)).cast(StringType))
            .selectExpr(stagingColumns: _*)
            .na.fill(0.0, Array("sales"))
            .na.fill(0, Array("units"))

          println(df1.cache().count())

          nullCheck(df1)
        }

      }
    }
  }

  test("dateFormatTest") {
    val str: String = "6/26/2015"

    println(new SimpleDateFormat("MM-dd-yyyy").format(str))
  }
}
