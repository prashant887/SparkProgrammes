import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{count, explode, split}

object WordCountDf {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("WordCountDf").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._

    val wordsDf=spark.read.textFile("/Users/pl465j/Documents/gitlearning.txt").
      withColumn("words",explode(split($"value"," "))).groupBy($"words").count()
    //wordsDf.show()
    wordsDf.filter($"count" > 30).show()
    wordsDf.createOrReplaceTempView("WordsTbl")
    spark.sql("select * from WordsTbl order by count desc").show()
  }


}
