import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object Check extends App {

  val conf               = new Configuration()
  val CoreSitePath       = new Path("core-site.xml")
  val HDFSSitePath       = new Path("hdfs-site.xml")
  val outputHDFSPath     = new Path("/batch")

  conf.addResource(CoreSitePath)
  conf.addResource(HDFSSitePath)

  val spark = SparkSession
    .builder()
    .appName("Batch Check")
    .config("spark.master", "local")
    .getOrCreate()

  val siteStatParquet =
    spark.read.load(s"$outputHDFSPath/2022-08-13").orderBy(desc("sum_count"))

  siteStatParquet.show(30)

}
