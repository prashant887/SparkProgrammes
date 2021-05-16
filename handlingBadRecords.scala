
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object handlingBadRecords {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("handlingBadRecords").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val fordData=spark.read
      //option("delimiter","|").
      .option("badRecordsPath","/tmp/corrupt/").
      json("/Users/pl465j/Downloads/dataset-master/ford_json.json")
    fordData.show(truncate = false)
  }
}
