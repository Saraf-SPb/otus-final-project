package batch

import com.typesafe.config.ConfigFactory
import model.SchemaPageTrafficSourceStat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object BatchLayer extends App {

  val spark = SparkSession
    .builder()
    .appName("Batch")
    .config("spark.master", "local")
    .getOrCreate()

  val configFactory = ConfigFactory.load()
  val inputParquetPath = configFactory.getString("config.speed_parquet_path")
  val outputParquetPath = configFactory.getString("config.batch_parquet_path")
  val outputBatchCheckpoint = configFactory.getString("config.batch_checkpoint")

  val siteStatParquet = spark.read.load(inputParquetPath)

  import spark.implicits._

  val inputDF = siteStatParquet.as[SchemaPageTrafficSourceStat]

  val pageTrafficSourceStat = inputDF
    .groupBy(col("page"), col("trafficSource"))
    .agg(sum("count").alias("sum_count"))

  pageTrafficSourceStat.orderBy(desc("sum_count")).show(30)

  private val outputPath: String = outputParquetPath +
    LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)

  pageTrafficSourceStat
    .write
    .format("parquet")
    .option("checkpointLocation", outputBatchCheckpoint)
    .mode(SaveMode.Overwrite)
    .save(outputPath)
}
