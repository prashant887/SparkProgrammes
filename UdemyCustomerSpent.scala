import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object UdemyCustomerSpent {

  def getFileds(line:String):(Int,Float)={
    val fileds=line.split(",")
    return (fileds(0).toInt,fileds(2).toFloat)
  }
  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sc=new SparkContext("local[*]","CustomerSpent")

    val lines=sc.textFile("D:\\SparkScalaUdemy\\customer-orders.csv")
    val AmtSpent=lines.map(x=>{getFileds(x)}).map(x=>{(x._1,x._2)}).reduceByKey((x,y)=>{x+y}).sortBy(x=>{x._2}).collect()

    println("\n Amount Spent\n")
    AmtSpent.foreach(x=>{println(x)})
val ItemGot=lines.map(x=>{x.split(",")(1)}).map(x=>{(x,1)}).reduceByKey((x,y)=>{x+y}).sortBy(x=>{x._2}).collect()
    println("\n ItemGot \n")
    ItemGot.foreach(x=>{println(x)})

  }
}
