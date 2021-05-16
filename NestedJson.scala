import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.functions.{explode,posexplode,explode_outer,posexplode_outer}

object NestedJson {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setAppName("nestedJson").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._

    val jsonData=spark.read.option("multiline","true").json("/Users/pl465j/Downloads/nested.json")
    val flat_data=jsonData.withColumn("flatdata",explode($"Education"))
      .drop($"Education").select($"name",$"flatdata.*")
    flat_data.printSchema()
flat_data.show(truncate = false)

    jsonData.show()



    spark.stop()

  }
}
