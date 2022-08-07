package stream

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.concurrent.duration.DurationInt

object SpeedLayer extends App {
  val spark = SparkSession
    .builder()
    .appName("Streaming kafka final")
    .master("local[2]")
    .getOrCreate()

  val schemaSiteStat = StructType(
    Array(
      StructField("timestamp", StringType),
      StructField("page", StringType),
      StructField("userId", StringType),
      StructField("duration", StringType),
      StructField("trafficSource", StringType)
    )
  )

  def readFromKafka(): DataFrame =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "input")
      .load()

  def transform(in: DataFrame): DataFrame =
    in
      .select(expr("cast(value as string) as actualValue"))
      .select(from_json(col("actualValue"), schemaSiteStat).as("page")) // composite column (struct)
      .selectExpr(
        "page.timestamp as timestamp",
        "page.page as page",
        "page.userId as userId",
        "page.duration as duration",
        "page.trafficSource as trafficSource"
      )
      .select(
        date_format(to_timestamp(col("timestamp"), "dd-MM-yyyy HH:mm:ss:SSS"), "HH:mm:ss:SSS")
          .as("time"),
        col("timestamp"),
        col("page"),
        col("userId"),
        col("duration"),
        col("trafficSource")
      )

  case class Record(timestampType: TimestampType, pageType: String)

  import spark.implicits._

  def transformToCaseClass(in: DataFrame): Dataset[String] = {
    in
      .select(expr("cast(value as string) as actualValue"))
      .as[String]
  }
  //      .map{ value => }

  def writeDfToParquet(in: DataFrame): Unit = {
    in
      .select("page", "trafficSource")
      .withColumn("timestamp", current_timestamp())
      .withWatermark("timestamp", "10 seconds")
      .groupBy(window($"timestamp", "10 seconds"),
        col("page"), col("trafficSource"))
      .agg(count("*").alias("count"))
      .drop("timestamp")
      .writeStream
      .format("parquet")
      .option("path", "out/final.parquet")
//      .option("checkpointLocation", "src/main/resources/checkpoint")
      .option("checkpointLocation", "tmp/streaming-checkpoint")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(10.seconds))
      .start()
//      .awaitTermination()
  }

  def writeDfToConsole(in: DataFrame): Unit =
    in
      .select("page", "trafficSource")
      .withColumn("timestamp", current_timestamp())
      .withWatermark("timestamp", "10 seconds")
      .groupBy(window($"timestamp", "10 seconds"),
        col("page"), col("trafficSource"))
      .agg(count("*").alias("count"))
      .drop("timestamp")
      .writeStream
      .format("console")
      .option("truncate", false)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(10.seconds))
      .start()
//      .awaitTermination()

  def writeDfToConsoleRAW(in: DataFrame): Unit =
    in
      .withColumn("timestamp", to_timestamp(col("timestamp"), "dd-MM-yyyy HH:mm:ss:SSS"))
      .withWatermark("timestamp", "10 seconds")
//      .groupBy(window($"timestamp", "10 seconds"),
//        col("page"), col("trafficSource"))
//      .agg(count("*").alias("count"))
      .writeStream
      .format("console")
      .option("truncate", false)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(10.seconds))
      .start()

  val frame       = readFromKafka()
  val transformed = transform(frame)

  writeDfToParquet(transformed)
//  writeDfToConsole(transformed)
//  writeDfToConsoleRAW(transformed)

  spark.streams.awaitAnyTermination()
}
