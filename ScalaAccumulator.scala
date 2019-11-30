import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable.ArrayBuffer

object ScalaAccumulator {

  def split(line:String):(Int,Array[Int])={
    val fileds=line.split("\\s+")

    val heroId=fileds(0).toInt

    var conns:ArrayBuffer[Int]=  ArrayBuffer()

    for(conn <- 1 to (fileds.length-1)){
      conns+=fileds(conn).toInt
    }
    return (heroId,conns.toArray)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val config=new SparkConf().setAppName("ScalaAccumulator").setMaster("local[*]")
    val sc=new SparkContext(config)

    val acc=sc.longAccumulator("Accumelator")

    val SourceFile=sc.textFile("D:\\TitanicData.csv").map(x=>{x.split(",")(1)})
      .filter(x=>{x.matches("\\d+")})

    SourceFile.foreach(x=>{println(x)})

    SourceFile.foreach(x=>acc.add(x.toInt))

    println("Total %d".format(acc.value))

    val SuperHeroData=sc.textFile("D:\\SparkScalaUdemy\\Marvel-graph.txt")
     //.map(x=>{split(x)}).mapValues(x=>{x.length})
        .map(x=>{x.split("\\s+")}).map(x=>{(x(0).toInt,x.length-1)})
    SuperHeroData.foreach(x=>{println(x)})
  }
}
