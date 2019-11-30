import java.nio.charset.CodingErrorAction

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object UdemyMostPopularMovie {

  //Read Movie Name File
  def loadMovieName():Map[String,String]={
    //Handle character encoding issue:
     implicit val codec=Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    //create a Map of Int,String

    var movieName:Map[String,String]=Map()
    var lines=Source.fromFile("D:\\ml-20m\\ml-20m\\movies.csv").getLines()
    for(line <- lines ){
      var fields=line.split(",")

      if(fields.length>1){
          movieName += (fields(0) -> fields(1))

      }
    }
   return movieName
  }
  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sc = new SparkContext("local[*]","MostPopularMovie")

    val line=sc.textFile("D:\\ml-20m\\ml-20m\\ratings.csv")
    // Create a broadcast variable of our ID -> movie name map
    val nameDict = sc.broadcast(loadMovieName)
    val popularMovie=line.map(x=>{(x.split(",")(1),1)}).reduceByKey((x,y)=>{x+y}).sortBy(x=>{x._2}).map(x=>{(nameDict.value(x._1),x._2)}).collect()
    println("\n PopularMovie \n")
    popularMovie.foreach(x=>{println(x)})

    sc.stop()
  }
}
