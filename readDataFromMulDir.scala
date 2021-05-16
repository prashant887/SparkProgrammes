import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name

object readDataFromMulDir {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("broadCasteVariable").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val path=List("/Users/pl465j/Downloads/LevelOne/InnerOne/*","/Users/pl465j/Downloads/LevelOne/InnerTwo/*")
    val DataFrame=spark.
      read.csv(path:_*).withColumn("file_name",input_file_name)

      DataFrame.select("file_name").distinct().show(truncate = false)


    val DataFrameAll=spark.
      read.csv("/Users/pl465j/Downloads/LevelOne/*").withColumn("file_name",input_file_name)

    DataFrameAll.select("file_name").distinct().show(truncate = false)
  }
}
