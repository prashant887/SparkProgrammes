import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object UdemyFilterWords {

  def getFileds(line:String):(Int,Float)={
    val field=line.split(",")
    val CustId=field(0).toInt
    val Amt=field(2).toFloat

    return (CustId,Amt)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sc = new SparkContext("local[*]","StopWord")
    val lines=sc.textFile("D:\\SparkScalaUdemy\\StopWordsTest.txt")
    val StopWords=Set("you","to")
    val Result=lines.flatMap(x=>{x.split("\\W+")}).map(x=>{x.toLowerCase}).filter(x=>{!StopWords.contains(x)}).map(x=>{(x,1)}).reduceByKey((x,y)=>{x+y}).sortBy(x=>{x._2}).collect()
    Result.foreach(x=>{println(x)})

    println("\n AmountSpent \n")
    val CustSrc=sc.textFile("D:\\SparkScalaUdemy\\MoneyByCustomers.txt")
    val AmtSpent=CustSrc.map(x=>{getFileds(x)}).map(x=>{(x._1,x._2)}).reduceByKey((x,y)=>{x+y}).sortBy(x=>{x._2}).collect()
    AmtSpent.foreach(x=>{println(x)})
    sc.stop()
  }
}
