import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level,Logger}
import org.apache.spark.sql.functions.{explode,posexplode,explode_outer,posexplode_outer,split}


object ExplodeExamples {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setAppName("ExplodeExamples").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._

    val ipData=Seq(("Hari","25","BE,MBA,HSC"),
      ("Azar","32",""),
      ("Kumar","35","ME,BE,Diploma"))
    println(ipData)

    val ipDf=ipData.toDF("Name","Age","Education")
    ipDf.show()

    ipDf.withColumn("edu",explode(split($"Education",",")))
    ipDf.select($"*",posexplode(split($"Education",","))).show()


    val marksDf=spark.read
      .option("header","true")
      .format("csv")
      .load("/Users/pl465j/Downloads/explode.csv")//.toDF("Name","Age","Education")
    marksDf.withColumn("Qualification",explode(split($"Education","\\|"))).show()
    marksDf.withColumn("Qualification",explode_outer(split($"Education","\\|"))).show()
    marksDf.select($"*",posexplode(split($"Education","\\|"))).show()
    marksDf.select($"*",posexplode_outer(split($"Education","\\|"))).show()


    spark.stop()

  }

}
