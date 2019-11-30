import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext


object UdemyMovieReview {

  def main(args: Array[String]): Unit = {
    println("Udemy Movie Review")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //Create a SparkContext using every core of local machine
    val sc=new SparkContext("local[*]","RatingCounter")

    //Load conents of the file to RDD
    val lines = sc.textFile("D:\\ml-20m\\ml-20m\\ratings.csv")
    println("Number of Lines %d".format(lines.count()))

    //Convert each line to string , split based on comma and extract 3rd filed
    val ratings = lines.map(x => {x.split(",")(2)})

    //count how many times each rating has occured
    val results=ratings.countByValue()

    //sort based on rating
    val sortedRes=results.toSeq.sortBy(x=>{x._1})
    sortedRes.foreach(x => {println(x)})

    println("\n ReduceByKey \n")

    val AltSol = lines.map(x => {x.split(",")(2)}).map(x=>{(x,1)}).reduceByKey((x,y)=> {x+y}).sortByKey(true,1).collect()
      //.sortByKey(true,2)
    //val AltSolSort= AltSol.sortByKey(true,1).collect()
    AltSol.foreach(x=>{println(x)})
  }
}
