package model

import org.apache.spark.sql.types._

case class SchemaSiteStat() {
  StructType(
    Array(
      StructField("timestamp", TimestampType),
      StructField("page", StringType),
      StructField("userId", StringType),
      StructField("duration", DoubleType),
      StructField("trafficSource", StringType)
    )
  )
}
