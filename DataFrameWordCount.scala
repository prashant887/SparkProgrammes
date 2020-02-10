import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{split,explode}
object DataFrameWordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setMaster("local[*]").setAppName("DataFrameWordCount")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._
    val RawDF=spark.read.format("csv").option("header","false").
      load("D:\\ExercisesSpark.txt").toDF("RawText")
    val wordsDF=RawDF.select(explode(split($"RawText"," ")).as("Words"))
    wordsDF.groupBy($"Words").count().show(truncate = false)
    sc.stop()
    spark.stop()
  }
}
