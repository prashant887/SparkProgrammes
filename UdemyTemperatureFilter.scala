import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.math.min
import scala.math.max
object UdemyTemperatureFilter {

def getFields(line:String):(String,String,Float)={
  val fileds=line.split(",")
  val stationID=fileds(0)
  val entryType=fileds(2)
  val temperature=fileds(3).toFloat*0.1f*(9.0f/5.0f)+32.0f
//  val temperature=fileds(3).toFloat
  return (stationID,entryType,temperature)
}
  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)
    val sc=new SparkContext("local[*]","Filter Temperature")
    val lines=sc.textFile("D:\\SparkScalaUdemy\\1800.csv")
    val tempRDD=lines.map(getFields)
    val minTemp=tempRDD.filter(x=>{x._2=="TMIN"}) //Only Keep TMIN records
    val statonTemp=minTemp.map(x=>{(x._1,x._3.toFloat)}) //Only Keep Station and Temperature
    val minTempByStat=statonTemp.reduceByKey((x,y)=>{min(x,y)})

    println("\n Min Temperature \n")
    minTempByStat.foreach(x=>{println(x)})

    val MaxTemp=tempRDD.filter(x=>{x._2=="TMAX"}).map(x=>{(x._1,x._3.toFloat)}).reduceByKey((x,y)=>{max(x,y)})

    println("\n Max Temperature \n")
    MaxTemp.foreach(x=>{println(x)})

    println("\n Multiple Filter \n")
    val MinMax=tempRDD.filter((x=>{x._2=="TMIN" || x._2=="TMAX"}))
    MinMax.foreach(x=>{println(x)})
  }

}
