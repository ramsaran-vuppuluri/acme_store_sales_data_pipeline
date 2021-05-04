package ca.acme.store.schema

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

trait EnrichmentSchema {
  val schema: StructType
  val partitionColumns: Seq[String]
}

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
  val partitionColumns: Seq[String] = Seq("province")
}