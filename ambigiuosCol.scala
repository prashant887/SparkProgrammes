
import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.SparkConf
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.functions.{to_json,from_json,json_tuple}

object ambigiuosCol {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("ambigiuosCol").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val jsonData = spark.read.option("multiline","true").json("/Users/pl465j/Downloads/input1.json").as("jsonData")
    jsonData.printSchema()
    println(jsonData.columns.mkString(","))
    jsonData.select($"_corrupt_record")
    jsonData.show(truncate = false)

  }
}
