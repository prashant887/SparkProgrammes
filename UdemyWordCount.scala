import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object UdemyWordCount {

  def main(args: Array[String]): Unit = {


    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sc= new SparkContext("local[*]","WordCount")

    val lines=sc.textFile("D:\\SparkScalaUdemy\\book.txt")
    val fileContants=lines.flatMap(x=>{x.split("\\W+")}).map(x=>{(x.toLowerCase,1)}).reduceByKey((x,y)=>{x+y}).sortBy(x=>{x._2}).collect()
    fileContants.foreach(x=>{println(x)})

    println("\n SortBy SortByKey \n")

    val SortByKey=lines.flatMap(x=>{x.split("\\W+")}).map(x=>{(x.toLowerCase,1)}).reduceByKey((x,y)=>{x+y}).map(x=>{(x._2,x._1)}).sortByKey().collect()
SortByKey.foreach(x=>{println(x)})

  }
}
