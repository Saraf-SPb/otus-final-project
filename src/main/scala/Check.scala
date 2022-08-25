import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}

object Check extends App {

  val configFactory               = ConfigFactory.load()
  val conf                        = new Configuration()
  val CoreSitePath                = new Path("core-site.xml")
  val HDFSSitePath                = new Path("hdfs-site.xml")
  val outputHDFSPath              = new Path("/batch")
  val outputParquetPathFromConfig = configFactory.getString("config.speed_parquet_path")

  conf.addResource(CoreSitePath)
  conf.addResource(HDFSSitePath)

  val spark = SparkSession
    .builder()
    .appName("Batch Check")
    .config("spark.master", "local")
    .getOrCreate()

//  val siteStatParquet =
//    spark.read.load(s"$outputHDFSPath/2022-08-12").orderBy(desc("sum_count"))
//
//  siteStatParquet.show(30)

  val outputParquetPath =
    "file:///" + System.getProperty("user.dir") + "/" + outputParquetPathFromConfig

  val s = spark.read.load(outputParquetPath)
  s.orderBy(desc("window")).show(numRows = 30, truncate = false)

}
