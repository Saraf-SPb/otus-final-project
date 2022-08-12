package batch

import com.typesafe.config.ConfigFactory
import model.SchemaPageTrafficSourceStat
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.net.URI
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object BatchLayer extends App {

  val configFactory = ConfigFactory.load()
  val inputParquetPath = configFactory.getString("config.speed_parquet_path")
  val outputParquetPath = configFactory.getString("config.batch_parquet_path")
  val outputBatchCheckpoint = configFactory.getString("config.batch_checkpoint")
  val baseHDFSPath = configFactory.getString("config.base_hdfs_path")

  val conf               = new Configuration()
  val CoreSitePath       = new Path("core-site.xml")
  val HDFSSitePath       = new Path("hdfs-site.xml")
  val outputHDFSPath     = new Path("/batch")
  private val fileSystem = FileSystem.get(new URI(baseHDFSPath), conf)

  conf.addResource(CoreSitePath)
  conf.addResource(HDFSSitePath)

  createFolder(s"$baseHDFSPath$outputHDFSPath")

  val spark = SparkSession
    .builder()
    .appName("Batch")
    .config("spark.master", "local")
    .getOrCreate()

  val siteStatParquet =
    spark.read.load("file:///" + System.getProperty("user.dir") + "/" + inputParquetPath)

  import spark.implicits._

  val inputDF = siteStatParquet.as[SchemaPageTrafficSourceStat]

  val pageTrafficSourceStat = inputDF
    .groupBy(col("page"), col("trafficSource"))
    .agg(sum("count").alias("sum_count"))

  pageTrafficSourceStat.orderBy(desc("sum_count")).show(30)

  private val outputPath: String = outputHDFSPath + "/" +
    LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
  pageTrafficSourceStat.write
    .format("parquet")
    .option("checkpointLocation", outputBatchCheckpoint)
    .mode(SaveMode.Overwrite)
    .save(outputPath)

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }
}
