import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf}

object CreatingUDF {

  def getGender(gen:String):String= gen.toLowerCase() match {
    case "m" => "Male"
    case "f" => "Female"
    case _ => "Unknown"
  }
  def main(args: Array[String]): Unit = {

    println(getGender("M"))
    println(getGender("F"))
    println(getGender("f"))
    println(getGender("m"))
    println(getGender("K"))

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("CreatingUDF").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._

    val genSql=spark.udf.register("geneder",getGender _ )

    val genFunc=(s:String)=> s.toUpperCase() match {
      case "M" => "Male"
      case "F" => "Female"
      case _ => "Unknown"
    }

    println(genFunc("M"))
    println(genFunc("F"))
    println(genFunc("f"))
    println(genFunc("m"))
    println(genFunc("K"))


    val genUdf=udf(genFunc)

    val data=Seq(("Amar","M"),("Vinay","m"),("Malini","F"),("July","m"),("Bobby","C"))

    val dataFrame=spark.createDataFrame(data).toDF("Name","Gender")
    dataFrame.withColumn("GenderExpandedSql",genSql($"Gender"))
      .withColumn("GenderExpanded",genUdf($"Gender"))
      .show()

    spark.stop()
    spark.close()


  }
}
