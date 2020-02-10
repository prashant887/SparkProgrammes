import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode,split}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType, DateType}
import org.apache.spark.sql.expressions.Window


object ExplodePiviot {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setMaster("local[*]").setAppName("ExplodePiviot")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val data=Seq((100,"Apple|Ball|Cat|Dog"),(200,"Egg|Fish|Ice|Jug|King"),(300,"Lion|Man|Nest"))

    var df=data.toDF("Id","Words")

    df.withColumn("Word",explode(split($"Words","\\|"))).drop("Words")
     .groupBy($"Id").pivot($"Word").count().show()

    df.printSchema()

  }

}
