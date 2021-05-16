import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.functions.{explode,concat_ws}
object PinCodeAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("PinCodes").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._

    /*
    val pinCodes=spark.read.json("/Users/pl465j/Downloads/pincode.json")
    val newData=pinCodes.select(explode($"data").as("data"))
    val data=newData.select(concat_ws(",",$"data").as("data"))
    val headers= pinCodes.select(concat_ws(",",$"fields.label").as("header"))
    val final_data=headers.union(data)
    final_data.show(truncate = false)
    */

    val postalPin=spark.read.format("csv")
      .option("header","true")
      .load("/Users/pl465j/Downloads/pincode.csv").as("allData")
    postalPin.filter($"Pincode"==="577401").show(truncate = false)
    val high=postalPin.groupBy($"Pincode").count()
      .as("Count").orderBy($"count".desc).limit(1).as("mostData")
    high.show()
    high.printSchema()
    val maxDf=high.join(postalPin,$"allData.Pincode"===$"mostData.Pincode").filter($"OfficeType"==="HO")
    maxDf.show()
    spark.stop()

  }

}
