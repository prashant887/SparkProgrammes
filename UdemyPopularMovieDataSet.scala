import java.nio.charset.{CoderMalfunctionError, CodingErrorAction}

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

import scala.io.{Codec, Source}

object UdemyPopularMovieDataSet {

  // Case class so we can get a column name for our movie ID
   case class Movie(movieID: Int)
  def loadMovieNames():Map[Int,String]={
    var movieNameMap:Map[Int,String]=Map()

    // Handle character encoding issues:
implicit  val codec=Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
val lines=Source.fromFile("D:\\ml-20m\\ml-20m\\movies.csv").getLines()

    for (line <- lines ){
      val fileds=line.split(",")

      if(fileds.length>1 && fileds(0).matches("\\d+")){
       movieNameMap+=(fileds(0).toInt -> fileds(1))
      }
    }
    return movieNameMap
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val config=new SparkConf()
      .setMaster("local[*]")
      .setAppName("UdemeyPopularMovieDataSet")

    val spark=SparkSession
      .builder()
      .config(config)
      .getOrCreate()

    val sc=spark.sparkContext

    val MovieData=sc.textFile("D:\\ml-20m\\ml-20m\\ratings.csv")

    val MovieId=MovieData.map(x=>{x.split(",")(1)})
        .filter(x=>{x.matches("\\d+")})
        .map(x=>{Movie(x.toInt)})

    import spark.implicits._

    val movieIdDF=MovieId.toDF

    movieIdDF.printSchema()

    val topMovies=movieIdDF.groupBy("MovieId").count().orderBy(desc("count"))

    println("\n TopMovies \n")

    topMovies.show()

    val topTenMovies=topMovies.take(10)

    println("\n Top 10 Movie Ids \n")
    topTenMovies.foreach(x=>{println(x)})

    val movieNames=loadMovieNames()

    for (movie <- topTenMovies ){
      println("Movie Name %s , Movie Count %d".format(movieNames(movie(0).toString.toInt),movie(1)))
    }
sc.stop()
    spark.stop()
  }
}
