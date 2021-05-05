package ca.acme.store.schema

/**
 * @author Ram Saran Vuppuluri
 *
 *         This file contains the schema objects for writing the enriched data.
 *
 *         Following are the Spark data types used by schema definitions in this file and their lower and upper limits.
 *
 *         * IntegerType: Represents 4-byte signed integer numbers. The range of numbers is from -2147483648 to 2147483647.
 *         * LongType: Represents 8-byte signed integer numbers. The range of numbers is from -9223372036854775808 to 9223372036854775807.
 *         * DoubleType: Represents 8-byte double-precision floating point numbers.
 *         * DateType: Represents values comprising values of fields year, month, day.
 *         * StringType: Represents character string values.
 *
 *         Reference Document: https://spark.apache.org/docs/2.4.0/sql-reference.html
 */

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

/**
 * Trait that determines the schema attributes for any data frame that is written into Enrichment zone.
 */
trait EnrichmentSchema {
  val schema: StructType
  val partitionColumns: Seq[String]
}

/**
 * This object the Transactions enrichment schema implementation.
 */
object TransactionEnrichmentSchema extends EnrichmentSchema {
  val schema = StructType(
    Array(
      StructField("collector_key", StringType, false), // Currently LongType will do fine but will fail if the value is changed to alpha numerical.
      StructField("trans_dt", DateType, false),
      StructField("sales", DoubleType, false),
      StructField("units", IntegerType, false),
      StructField("trans_key", StringType, false), // Currently LongType will do fine but will fail if the value is changed to alpha numerical.
      StructField("trans_year", IntegerType, false),
      StructField("trans_month", IntegerType, false),
      StructField("product_key", StringType, false), // Currently LongType will do fine but will fail if the value is changed to alpha numerical.
      StructField("sku", StringType, false), // Currently IntegerType will do fine but will fail if the value is changed to alpha numerical.
      StructField("upc", StringType, false), // Currently LongType will do fine but will fail if the value is changed to alpha numerical.
      StructField("item_name", StringType, false),
      StructField("item_description", StringType, false),
      StructField("department", StringType, false),
      StructField("category", StringType, false),
      StructField("store_location_key", StringType, false), // Currently IntegerType will do fine but will fail if the value is changed to alpha numerical.
      StructField("region", StringType, false),
      StructField("province", StringType, false),
      StructField("city", StringType, false),
      StructField("postal_code", StringType, false),
      StructField("banner", StringType, false),
      StructField("store_num", IntegerType, false)
    )
  )
  val partitionColumns: Seq[String] = Seq("province", "trans_year", "trans_month")
}