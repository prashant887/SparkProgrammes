import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession





object movieLenseAnalysis {

  case class rating(UserID:Int,MovieID:Int,Rating:Int,Timestamp:Long)
  case class movie(MovieID:Int,Title:String,Genres:String)

  def paraseRating(rawData:String):rating={
    val ary=rawData.split("::")
    rating(ary(0).toInt,ary(1).toInt,ary(2).toInt,ary(3).toInt)
  }

  def parseMovie(rawData:String):movie={
    val ary=rawData.split("::")
    movie(ary(0).toInt,ary(1),ary(2))
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("parseMovie").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val sc=spark.sparkContext
    val ratingData=sc.textFile("/Users/pl465j/Downloads/ml-1m/ratings.dat")
    val movieData=sc.textFile("/Users/pl465j/Downloads/ml-1m/movies.dat")
    val userData=sc.textFile("/Users/pl465j/Downloads/ml-1m/users.dat")

    ratingData.map(x=>{paraseRating(x)}).map(x=>{(x.MovieID,(x.Rating,1))})
      .reduceByKey((x,y)=>{(x._1+y._1,x._2+y._2)}).mapValues(x=>{(x._2,x._1.toFloat/x._2.toFloat)})
      .collect()
      .foreach(x=>{println(x._1,x._2._1,x._2._2)})

    println("\n \n \n")

    movieData.
      map(x=>{parseMovie(x)}).
      flatMap(x=>{x.Genres.split("\\|")})
      .map(x=>{(x,1)})
     .reduceByKey((x,y)=>{x+y})
      .collect()
      .foreach(x=>{println(x)})

    println("\n \n \n")

    val topRdd=ratingData.map(x=>{paraseRating(x)})
      .map(x=>{(x.MovieID,x.Rating)})
      .reduceByKey((x,y)=>{x+y})
      .sortBy(x=>{x._2},false)

    val movieNames=movieData.map(x=>{parseMovie(x)}).map((x=>{(x.MovieID,x.Title)}))

      topRdd.take(10).foreach(x=>{println(x)})

    println("\n \n \n")
    topRdd.join(movieNames).sortBy(x=>{x._2._1},false)
      .take(10)
      .foreach(x=>{println(x._2)})



    sc.stop()
    spark.stop()
    spark.close()
  }

}
