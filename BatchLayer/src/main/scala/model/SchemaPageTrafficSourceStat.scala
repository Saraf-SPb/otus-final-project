package model

import org.apache.spark.sql.types._

case class SchemaPageTrafficSourceStat () {
  StructType(
    Array(
      StructField("window", TimestampType),
      StructField("page", StringType),
      StructField("trafficSource", StringType),
      StructField("count", IntegerType)
    )
  )
}
