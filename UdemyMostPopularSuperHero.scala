import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object UdemyMostPopularSuperHero {

  def countCoOccurences(line:String):(Int,Int)={
    val fields=line.split("\\s+")
    return(fields(0).trim().toInt,fields.length-1)
  }
  def parseName(name:String):Option[(Int,String)]={
    val fileds=name.split('\"')

    if(fileds.length>1){
      return Some((fileds(0).trim().toInt,fileds(1)))
    }
    else{
      return  None
    }
  }//flatMap will Ignore None results and only consider some
  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc=new SparkContext("local[*]","MostPopularSuperHero")


    val PopularSuperHero=sc.textFile("D:\\SparkScalaUdemy\\Marvel-graph.txt")
      .map(x=>{countCoOccurences(x)}).reduceByKey((x,y)=>{x+y})

    val MostPopularSuperHero=PopularSuperHero
      .reduce((x,y)=>{if(x._2>y._2) x else y })

    val names=sc.textFile("D:\\SparkScalaUdemy\\Marvel-names.txt").flatMap(x=>{parseName(x)})

    println("Most Popular Super Hero %s with %d appearance"
      .format(names.lookup(MostPopularSuperHero._1)(0),MostPopularSuperHero._2))

    val MaxPopularHero=PopularSuperHero.map(x=>{(x._2,x._1)})//swap to Make count as key
      .max() //Max based on key

    println("Max Popular Super Hero %s with %d appearance"
      .format(names.lookup(MaxPopularHero._2)(0),MaxPopularHero._1))

      val ListOfSuperHeros=PopularSuperHero.sortBy(x=>{x._2}).collect().map(x=>{(names.lookup(x._1)(0),x._2)})

      ListOfSuperHeros.foreach(x=>{println("%s SuperHero has appeared %d times".format((x._1),x._2))})




    //    names.foreach(x=>{println(x)})


    sc.stop()
  }
}
