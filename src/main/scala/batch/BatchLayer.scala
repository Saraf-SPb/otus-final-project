package batch

import model.SchemaPageTrafficSourceStat
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object BatchLayer extends App {

  val spark = SparkSession
    .builder()
    .appName("Batch")
    .config("spark.master", "local")
    .getOrCreate()

  val siteStatParquet = spark.read.load("out/final.parquet")

//  siteStatParquet.orderBy("window").show(false)

  import spark.implicits._

  val inputDF = siteStatParquet.as[SchemaPageTrafficSourceStat]
//  inputDF.show(false)

  val pageTrafficSourceStat = inputDF
    .groupBy(col("page"),col("trafficSource"))
    .agg(sum("count").alias("sum_count"))

  pageTrafficSourceStat.orderBy(desc("sum_count")).show(30)

  private val outputPath: String = "out/pageTrafficSourceStat.parquet/" +
    LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)

  pageTrafficSourceStat
    .write
    .format("parquet")
    .option("checkpointLocation", "/tmp/batch-checkpoint")
    .mode(SaveMode.Overwrite)
    .save(outputPath)



//  val inputSiteStatDF = siteStatParquet.as[SchemaSiteStat]

//  private val siteStat: DataFrame = inputSiteStatDF
//    .groupBy(col("page"))
//    .agg(
//      avg("duration").alias("avg_duration"),
//      max("duration").alias("max_duration"),
//      min("duration").alias("min_duration"),
//      count("page")
//    )

//  private val siteStat = inputSiteStatDF
//    .withColumn("d", col("duration").cast("Double"))
//    .groupBy(col("page"))
//    .agg(
//      avg("d").alias("avg_duration"),
//      max("d").alias("max_duration"),
//      min("d").alias("min_duration")
//    )
//
//  private val trafficSourceStat = inputSiteStatDF
//    .groupBy(col("trafficSource"))
//    .agg(count("*").alias("count"))
//
//  println(inputSiteStatDF.count())
//  inputSiteStatDF.show(false)
//  siteStat.show()
//  trafficSourceStat.show(false)
}
