import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.SparkConf
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.functions.{to_json,from_json,json_tuple}

object jsonInCsv {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("jsonInCsv").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val jsonData=spark.read.format("csv").option("escape","\"").option("header","true")
      .load("/Users/pl465j/Downloads/dummy2.csv").as("jsonData")
    jsonData.printSchema()
    jsonData.show(truncate = false)
    val extractedData=jsonData.select($"*",json_tuple($"request","Response").as("extracted"))
    extractedData.printSchema()
    extractedData.show(truncate = false)
    val flattnedData=extractedData.drop("request")
      .select($"*",json_tuple($"extracted","MessageId","Latitude","longitude").as(Array("MessageId","Latitude","longitude")))
      .drop("extracted")
    flattnedData.printSchema()
    flattnedData.show(truncate = false)

  }
}
