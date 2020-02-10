import org.apache.log4j.{Level,Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array,when}
object ConcatWS {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setMaster("local[*]").setAppName("ConcatWS")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    var df = Seq(
      ("i like blue and red"),
      ("you pink and blue")
    ).toDF("words1")

    df=df.withColumn(
      "colors",
      array(
        when ($"words1".contains("blue") ,"blue"),
          when($"words1".contains("red"),"red"),
        when($"words1".contains("pink"),"pink"),
        when($"words1".contains("cyan"),"cyan")
      )
      )
    df.show(truncate = false)

    spark.stop()
  }

}
