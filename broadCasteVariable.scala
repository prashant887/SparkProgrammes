import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.SparkConf
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.functions.{regexp_replace,when,udf}

object broadCasteVariable {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("broadCasteVariable").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val sc=spark.sparkContext
    val usPopulation=spark.read.format("csv").
      option("header","true").
      option("delimiter","|").
      load("/Users/pl465j/Downloads/dataset-master/uspopulation.csv")
    usPopulation.show(truncate = false)

    val Sates=Seq(("TX","Texas"),
      ("NY","New York"),
      ("CA","California"),
     ("AZ","Arizona"),
       ("OH","Ohio"),
      ("CO","Collarado"))

    val SatesMap=Map("TX" -> "Texas",
    "NY" -> "New York",
    "CA" -> "California",
    "AZ" -> "Arizona",
    "OH" -> "Ohio",
      "IL" -> "Illoina",
      "CO" ->  "Collarado")

    val SatesDF=spark.createDataFrame(Sates).toDF("Abbr","Name")
    val broad=sc.broadcast(SatesMap)
    println(broad.value("CA"))

    val funUDF=udf((s:String)=>{broad.value(s)})

    usPopulation.withColumn("SateName",funUDF($"State_Code")).show(truncate = false)

    broad.unpersist()
    usPopulation.withColumn("SateName",funUDF($"State_Code")).show(truncate = false)

    broad.destroy()
    usPopulation.withColumn("SateName",funUDF($"State_Code")).show(truncate = false)



    sc.stop()
    spark.stop()
    spark.close()



  }

  /*
  For joins data will be in executor / other nodes so each time data needs to picked from dirver
  brodcase will push data to all executors
   */

}
