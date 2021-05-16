import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.functions.{explode,split}


object SplittingColValues {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("SplittingColValues").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val data=Seq((100,"Apple|Ball|Cat|Dog"),(200,"Egg|Fish|Ice|Jug|King"),(300,"Lion|Man|Nest"))
    val df=data.toDF("Id","Words")
    df.show()
    df.select(explode(split($"Words","\\|"))).show()

  }

}
