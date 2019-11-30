import java.nio.charset.CodingErrorAction

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.{Codec, Source}
import scala.math.sqrt
/** Load up a Map of movie IDs to movie names. */
object UdemyMovieSimilarties {

  def loadMovieNames():Map[Int,String]={
    // Handle character encoding issues:
    implicit val codec=Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var MoveNameMap:Map[Int,String]=Map()

    val lines=Source.fromFile("D:\\ml-20m\\ml-20m\\movies.csv").getLines()

    for (line <- lines ){
      val fileds=line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
      if(fileds(0).matches("\\d+")){
        MoveNameMap+=(fileds(0).toInt->fileds(1))
      }
    }

    return  MoveNameMap
  }

  def parseFile(line:String):(String,String,String,String)={
    val fileds=line.split(",")


      return (fileds(0),fileds(1),fileds(2),fileds(3))

  }

  type MovieRating=(Int,Double)
  type UserRatingPair=(Int,(MovieRating,MovieRating))

  def filterDuplicates(userRatings:UserRatingPair):Boolean={
    val movieRating1=userRatings._2._1
    val movieRating2=userRatings._2._2

    val movieId_1=movieRating1._1
    val movieId_2=movieRating2._1

    val userId=userRatings._1

    return movieId_1 < movieId_2
  }

  def makePairs(userRating: UserRatingPair):((Int,Int),(Double,Double))={
    val movieRating1= userRating._2._1
    val movieRating2=userRating._2._2

    val movieId1=movieRating1._1
    val Rating1=movieRating1._2

    val movieId2=movieRating2._1
    val Rating2=movieRating2._2

    return ((movieId1,movieId2),(Rating1,Rating2))
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs:RatingPairs):(Double,Int)={
    var numPairs:Int=0
    var sum_xx:Double=0.0
    var sum_yy:Double=0.0
    var sum_xy:Double=0.0

    for(pairs <- ratingPairs){
      val ratingX=pairs._1
      val ratingY=pairs._2

      sum_xx+=ratingX*ratingX
      sum_yy+=ratingY*ratingY
      sum_xy+=ratingX*ratingY
      numPairs+=1
    }
    val numberator:Double=sum_xy
    val denominator=sqrt(sum_xx)*sqrt(sum_yy)

    var score:Double=0.0
    if(denominator != 0.0){
      score=numberator/denominator
    }

    return (score,numPairs)
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val config=new SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
    val sc = new SparkContext(config)

    val MovieNameMap=loadMovieNames()

    for(key <- MovieNameMap.keys){
      println("Movie Id %d : Movie Name %s".format(key,MovieNameMap(key)))
    }

  //  val MovieData=sc.textFile("D:\\ml-20m\\ml-20m\\ratings.csv")
      val MovieData=sc.textFile("D:\\smallmoviereating.csv")

    val movieDataFileds=MovieData.mapPartitionsWithIndex((idx,rec)=>{if(idx==0){rec.drop(1)}else{rec}})
        .map(x=>{parseFile(x)})
        .map(x=>{(x._1.toInt,(x._2.toInt,x._3.toDouble))})  // Map ratings to key / value pairs: user ID => movie ID, rating

    // Emit every movie rated together by the same user. Self-join to find every combination.

    val joinedRating=movieDataFileds.join(movieDataFileds)

    // At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

    // Filter out duplicate pairs

val uniqueJoinedData=joinedRating.filter(x=>{filterDuplicates(x)})

    // Now key by (movie1, movie2) pairs.
    val moviePairs=uniqueJoinedData.map(x=>{makePairs(x)})


    // We now have (movie1, movie2) => (rating1, rating2)
    // Now collect all ratings for each movie pair and compute similarity
 val movieRatingPair=moviePairs.groupByKey()
    movieRatingPair.foreach(x=>{println(x._2)})
    // We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    // Can now compute similarities.

    val moviePaidSimilarities=movieRatingPair.mapValues(x=>{computeCosineSimilarity(x)}).cache()

    moviePaidSimilarities.foreach(x=>{println(x)})

    //Save the results if desired
    //val sorted = moviePairSimilarities.sortByKey()
    //sorted.saveAsTextFile("movie-sims")

    // Extract similarities for the movie we care about that are "good".

  }
}
