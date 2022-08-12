package stream

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SpeedLayer extends App {
  val spark = SparkSession
    .builder()
    .appName("Streaming kafka final")
    .master("local[2]")
    .getOrCreate()

  val configFactory = ConfigFactory.load()
  val outputParquetPathFromConfig = configFactory.getString("config.speed_parquet_path")
  val outputStreamingCheckpointFromConfig = configFactory.getString("config.streaming_checkpoint")
  val windowDuration = configFactory.getInt("config.window_duration").toString + " seconds"
  val triggerProcessingTime = configFactory.getInt("config.trigger_processing_time").toString + " seconds"

  val outputParquetPath = "file:///" + System.getProperty("user.dir") + "/" + outputParquetPathFromConfig
  val outputStreamingCheckpoint = "file:///" + System.getProperty("user.dir") + "/" + outputStreamingCheckpointFromConfig

  var schemaSiteStat: StructType = StructType(
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
      .option("kafka.bootstrap.servers", configFactory.getString("config.kafka_bootstrap_servers"))
      .option("subscribe", configFactory.getString("config.topic"))
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
        col("page"),
        col("userId"),
        col("duration"),
        col("trafficSource")
      )

  import spark.implicits._

  def writeDfToParquet(in: DataFrame): Unit = {
    in
      .select("page", "trafficSource")
      .withColumn("timestamp", current_timestamp())
      .withWatermark("timestamp", windowDuration)
      .groupBy(window($"timestamp", windowDuration),
        col("page"), col("trafficSource"))
      .agg(count("*").alias("count"))
      .drop("timestamp")
      .writeStream
      .format("parquet")
      .option("path", outputParquetPath)
      .option("checkpointLocation", outputStreamingCheckpoint)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(triggerProcessingTime))
      .start()
  }

  def writeDfToConsole(in: DataFrame): Unit =
    in
      .select("page", "trafficSource")
      .withColumn("timestamp", current_timestamp())
      .withWatermark("timestamp", windowDuration)
      .groupBy(window($"timestamp", windowDuration),
        col("page"), col("trafficSource"))
      .agg(count("*").alias("count"))
      .drop("timestamp")
      .writeStream
      .format("console")
      .option("truncate", false)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(triggerProcessingTime))
      .start()

  val frame       = readFromKafka()
  val transformed = transform(frame)

  writeDfToParquet(transformed)
//  writeDfToConsole(transformed)
//  writeDfToConsoleRAW(transformed)

  spark.streams.awaitAnyTermination()
}
