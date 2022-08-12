import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object Check extends App {

  val conf               = new Configuration()
  val CoreSitePath       = new Path("core-site.xml")
  val HDFSSitePath       = new Path("hdfs-site.xml")

  conf.addResource(CoreSitePath)
  conf.addResource(HDFSSitePath)

  val spark = SparkSession
    .builder()
    .appName("Batch Check")
    .config("spark.master", "local")
    .getOrCreate()

  val configFactory    = ConfigFactory.load()
  val inputParquetPath = configFactory.getString("config.batch_parquet_path")

  val siteStatParquet =
    spark.read.load(s"$inputParquetPath/2022-08-12").orderBy(desc("sum_count"))

  siteStatParquet.show(30)

}
