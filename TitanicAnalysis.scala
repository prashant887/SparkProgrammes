import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object TitanicAnalysis {

  def parseFields(line:String):(String,String,String,String,String,String,String,String,String,String,String,String)={
    val fileds=line.split(",")

   val PassengerId = fileds(0)
    val Survived= fileds(1)
    val Pclass= fileds(2)
    val Name = fileds(3)
    val Sex= fileds(4)
    val Age = fileds(5)
    val SibSp= fileds(6)
    val Parch = fileds(7)
    val Ticket= fileds(8)
    val Fare= fileds(9)
    val Cabin= fileds(10)
    val Embarked= fileds(11)

return (PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sc=new SparkContext("local[*]","TitanicAnalysis")

    val TitanicData=sc.textFile("D:\\TitanicData.csv")

    val TitanicAnalysis = TitanicData.map(x=>{parseFields(x)})
      .filter(x=>{x._2.matches("\\d+") && x._6.matches("\\d+")})
      .map(x=>{(x._5,x._6.toInt)})
        .mapValues(x=>{(x,1)})
        .reduceByKey((x,y)=>{(x._1+y._1,x._2+y._2)})
        .mapValues(x=>{x._1/x._2.toFloat})

    println("\n Average age of Died People \n")
    TitanicAnalysis.foreach(x=>{println(x)})

    println("\n Number of People Died Survived Per Class")

    val PeopleSurvivedPerClass=TitanicData.map(x=>{parseFields(x)})
      .filter(x=>{x._6.matches("\\d+") && x._3.matches("\\d+") && x._2.matches("\\d+")})
        .map(x=>{((x._2.toInt,x._3.toInt,x._5,x._6.toInt),1)})
        .reduceByKey((x,y)=>{x+y})
        .sortByKey()



    PeopleSurvivedPerClass.foreach(x=>{println(x._1._1,x._1._2,x._1._3,x._1._4,x._2)})

    sc.stop()
  }

}
