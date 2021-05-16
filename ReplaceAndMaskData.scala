import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.SparkConf
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.functions.{regexp_replace,when,udf}

object ReplaceAndMaskData {

  def maskEmail(emaild:String): String ={

    val splitStr=emaild.split("@")(0).toCharArray
    var a=""

    for (i <- 0 to splitStr.length -1 ) {
      if (i==0 | i==splitStr.length-1){
        a=a+splitStr(i)
      }
      else {
        a=a+'*'
      }
    }
     a+'@'+emaild.split("@")(1)

  }

  def mask_mobile(mobnumIp:String):String={
    val mobnum=mobnumIp.toCharArray

    var a=""

    for ( i <- 0 to mobnum.length-1){
      if (i<2 | i>mobnum.length-3) {
        a+=mobnum(i).toString
      }
      else {
        a+='*'
      }
    }
    a
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("ReplaceAndMaskData").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val bankData=spark.read.format("csv").
      option("header","true").
      load("/Users/pl465j/Downloads/dataset-master/BankChurners.csv")
    bankData.select(regexp_replace($"Marital_Status","Unknown","Single")).show()
    bankData.select(when($"Marital_Status"==="Unknown" , "Single").otherwise($"Marital_Status")).show()

    // bankData.show(truncate = false)

    val maskData=spark.read.format("csv").
      option("header","true").
      load("/Users/pl465j/Downloads/dataset-master/mask_data.csv")
    maskData.show(truncate = false)

    println(maskEmail("prashant887@gmail.com"))
    println(mask_mobile("9632908822"))

    val maskEmailUdf=spark.udf.register("maskEmail",maskEmail _)
    val maskmobUdf=spark.udf.register("maskMob",mask_mobile _)

    maskData.withColumn("maskedEmail",maskEmailUdf($"email"))
      .withColumn("maskedMobile",maskmobUdf($"mobile"))
      .drop($"mobile")
      .drop($"email")
      .show(truncate = false)

    spark.stop()
    spark.close()
  }

}
