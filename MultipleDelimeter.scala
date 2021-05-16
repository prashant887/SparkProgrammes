import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger,Level}

object MultipleDelimeter {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("DifferentReadModes").setMaster("local[2]")
    val spark=SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val mulDelData=spark.read.option("header","true").option("delimiter","~|")
      .format("csv").load("/Users/pl465j/Downloads/muldel.csv")
    mulDelData.show(truncate = false)

    val mulDelDataTxt=spark.read.text("/Users/pl465j/Downloads/muldel.csv")
    mulDelDataTxt.show(truncate = false)
    val header=mulDelDataTxt.first()(0).toString
    println(header)
    val schema=header.split("~\\|")
    println(schema.mkString(","))
    val newDF=mulDelDataTxt.filter($"value"=!=header).rdd.map(x=>{x(0).toString.split("~\\|")}).map(x=>{(x(0),x(1))}).toDF(schema:_*)
    println("New DF")
    newDF.show()
    //newDF.collect().foreach(x=>{println(x.mkString("--"))})
    spark.stop()
    spark.close()
  }
}
