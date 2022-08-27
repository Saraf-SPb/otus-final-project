package stream

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SpeedLayer extends App {

  // Величина параллелизма
  private val slots = 2
  val spark = SparkSession
    .builder()
    .appName("Streaming kafka final")
    .master(s"local[$slots]")
    .getOrCreate()

  // Загружаем конфигурацию из файлов refecence.conf и application.conf
  val configFactory                       = ConfigFactory.load()
  val outputParquetPathFromConfig         = configFactory.getString("config.speed_parquet_path")
  val outputStreamingCheckpointFromConfig = configFactory.getString("config.streaming_checkpoint")
  val windowDuration                      = configFactory.getInt("config.window_duration").toString + " seconds"
  val Watermark                           = configFactory.getInt("config.window_watermark").toString + " seconds"
  val triggerProcessingTime =
    configFactory.getInt("config.trigger_processing_time").toString + " seconds"

  // parquet будем записывать в файловую систему просто для разнообразия. Разумнее писать в HDFS
  // Для этого нужно убрать file:/// из путей при подключенной конфигуации HDFS
  val outputParquetPath =
    "file:///" + System.getProperty("user.dir") + "/" + outputParquetPathFromConfig
  val outputStreamingCheckpoint =
    "file:///" + System.getProperty("user.dir") + "/" + outputStreamingCheckpointFromConfig

  // Структура данных получаемая из топика kafka
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

  // Трансформация JSON-данных из Kafka
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
        to_timestamp(col("timestamp"), "dd-MM-yyyy HH:mm:ss:SSS").as("timestamp"),
        col("page"),
        col("userId"),
        col("duration"),
        col("trafficSource")
      )

  import spark.implicits._

  // Оставлю для примера записи в parquet из стрима
  def writeDfToParquet(in: DataFrame): Unit = {
    in.writeStream
      .format("parquet")
      .option("path", outputParquetPath)
      .option("checkpointLocation", outputStreamingCheckpoint)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(triggerProcessingTime))
      .start()
  }

  /**
   * Расчет статистики. Считаем на какую страницу из какого источника как часто переходят пользователи
   * @param in - входящий DF
   * @return
   */
  def transformDf(in: DataFrame) = {
    in
      .withWatermark("timestamp", Watermark)
      .groupBy(window($"timestamp", windowDuration), col("page"), col("trafficSource"))
      .agg(count("*").alias("count"))
  }

  // Оставлю для примера записи из стрима в консоль.
  // Если outputMode указать append - почему-то пишет пустой DF, пока не понял почему.
  def writeDfToConsoleRAW(in: DataFrame): Unit = {
    in.writeStream
      .format("console")
      .option("truncate", false)
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(triggerProcessingTime))
      .start()
  }

  /**
   * Запись данных в целевые хранилища.
   * В текущей реализации происходит запись статитики в HDFS + сырые данные в  Cassandra
   * @param in - входящий DF
   */
  def writeDfToTarget(in: DataFrame): Unit =
    in.writeStream
      .foreachBatch(alterForeachBatch _)
      .trigger(Trigger.ProcessingTime(triggerProcessingTime))
      .start()

  def alterForeachBatch(batch: DataFrame, batchId: Long): Unit = {
    batch.persist()
    batch.count()

    batch.repartition(slots)
    batch
      .write
      .cassandraFormat("click", "public")
      .mode(SaveMode.Append)
      .save()

    val transform = transformDf(batch)
    transform.count()

    transform
      .coalesce(10)
      .write
      .mode(SaveMode.Append)
      .option("checkpointLocation", outputStreamingCheckpoint)
      .parquet(outputParquetPath)

    batch.unpersist()
  }

  val frame: DataFrame       = readFromKafka()
  val transformed: DataFrame = transform(frame)
  writeDfToTarget(transformed)

  spark.streams.awaitAnyTermination()
}
