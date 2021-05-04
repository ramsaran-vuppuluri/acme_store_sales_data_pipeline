name := "acme_store_sales_data_pipeline"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
libraryDependencies += "com.google.cloud" % "google-cloud-storage" % "1.113.16"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test