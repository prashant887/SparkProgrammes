import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.functions.{explode}
object YelpAnaysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("YelpAnlysis").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._

    val yelpData=spark.read.json("/Users/pl465j/Downloads/yelp_academic_dataset_business.json")
    yelpData.printSchema()
    yelpData.select($"business_id",$"hours.Monday").show(false)
yelpData.filter( $"business_id"==="bvN78flM8NLprQ1a1y5dRg").select($"hours").withColumn("working",explode($"hours"))
  .show(truncate = false)
    spark.close()

  }
}
