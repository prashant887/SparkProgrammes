import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce,lit}
object CoalesceSql {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("CoalesceSql").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val phoneData=spark.read.format("csv").
      option("header","true").
      //option("delimiter","|").
      load("/Users/pl465j/Downloads/dataset-master/mob_num.csv")
    phoneData.show(truncate = false)
    phoneData.withColumn("PhoneNo",coalesce($"Personal_Mobile",$"Home_Mobile",$"office_no",lit("Not Avilable"))).show()
    spark.stop()
    spark.close()
  }
}
