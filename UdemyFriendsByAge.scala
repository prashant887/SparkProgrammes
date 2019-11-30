import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
object UdemyFriendsByAge {

  def parseLine(line:String)={
    val fileds=line.split(",")
    val age=fileds(2).toInt
    val firends=fileds(3).toInt

    (age,firends)
  }

  def getFirstNameFriends(line:String):(String,Int)={
    val fileds = line.split(",")
    val firstName=fileds(1).toString
    val firends=fileds(3).toInt

    (firstName,firends)
  }
  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sc = new SparkContext("local[*]","FriendsByAge")
    val lines=sc.textFile("D:\\SparkScalaUdemy\\fakefriends.csv")
    println(lines.count())
    val rdd=lines.map(x=>{parseLine(x)})
    val RddByAge=rdd.mapValues(x=>{(x,1)}) //Does Operation of Value Tuple
    RddByAge.foreach(x=>{println(x)})
    val totalByAge=RddByAge.reduceByKey((x,y)=>{(x._1+y._1,y._2+x._2)})
    //val averagesByAge = totalByAge.mapValues(x=>{x._2})
   // val SortVal = totalByAge.sortBy(x=>{x._2._2})
    //SortVal.foreach(x=>{println(x)})
    totalByAge.foreach(x=>{println(x)})
   val averagesByAge = totalByAge.mapValues(x => x._1 / x._2)
    println("\n Average By Value \n")
    averagesByAge.foreach(x=>{println(x)})

    val SortedAvgVal=averagesByAge.sortBy(x=>{x._2})

    println("\n Sorted Values \n")
    SortedAvgVal.foreach(x=>{println(x)})

    println("\n Friends By FirstName \n")
    val FriendsByName=lines.map(getFirstNameFriends).mapValues(x=>{(x,1)}).reduceByKey((x,y)=>{(x._1+y._1,x._2+y._2)}).mapValues(x=>{x._1/x._2}).sortBy(x=>{x._2})
    FriendsByName.foreach(x=>{println(x)})
    sc.stop()
  }
}
