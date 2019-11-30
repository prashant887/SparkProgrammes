import java.nio.charset.CodingErrorAction

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation._
import scala.io.{Codec, Source}

object UdemyMovieRecomandationMLLib {

  def loadMovieNames():Map[Int,String]={

    var MovieMap:Map[Int,String]=Map()

    implicit  val codec=Codec("UTF-8")
codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var lines=Source.fromFile("D:\\ml-20m\\ml-20m\\movies.csv").getLines()

    for(line <- lines ){
      val filed=line.split(",")
      if(filed.length>0 && filed(0).matches("\\d+")){
        MovieMap+=(filed(0).toInt -> filed(1))
      }
    }

    return MovieMap
  }
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc=new SparkContext("local[*]","UdemyMovieRecomandationMLLib")

val movieNameDict=loadMovieNames()

    val ratingdata=sc.textFile("D:\\ml-20m\\ml-20m\\ratings.csv")

    val rating=ratingdata.map(x=>{x.split(",")})
      .filter(x=>{x(0).matches("\\d+") && x(1).matches("\\d+")})
      .map(x=>{Rating(x(0).toInt,x(1).toInt,x(2).toDouble)}).cache()

    // Build the recommendation model using Alternating Least Squares
    println("\nTraining recommendation model...")

    val rank = 8
    val numIterations = 20

    val model=ALS.train(rating,rank,numIterations)

    val userId=100

    println("\nRatings for user ID " + userId + ":")

    val userRating=rating.filter(x=>{x.user==userId})

    for (ratings <- userRating )
      {
        println( "Movie Name %s : rating %s".format( movieNameDict(ratings.product.toInt),ratings.rating.toString))
      }

    println("\nTop 10 recommendations:")

    val reccs=model.recommendProducts(userId,10)

    for(recc <- reccs ){
      println("Movie %s and score %s".format(movieNameDict(recc.product.toInt),recc.rating.toString))
    }

    sc.stop()
  }
}
