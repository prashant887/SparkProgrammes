import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
object OlympicData {

  def parseData(lines:String):(String,String,String,String,String,String,String,String,String,String)={

    val fileds=lines.split(",")
    val Name=fileds(0)
    val Age=fileds(1)
    val Country=fileds(2)
    val Year=fileds(3)
    val CloseDate=fileds(4)
    val Sports=fileds(5)
    val Gold=fileds(6)
    val Silver=fileds(7)
    val Bronze=fileds(8)
    val TotalMedal=fileds(9)

    return (Name,Age,Country,Year,CloseDate,Sports,Gold,Silver,Bronze,TotalMedal)

  }
  def main(args: Array[String]): Unit = {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)
    val sc= new SparkContext("local[*]","OlymicData")
    val lines=sc.textFile("D:\\olympix_data.csv")
    val fileds=lines.map(x=>{parseData(x)})
    val SwimmimgMedals=  fileds.filter(x=>{x._6.equalsIgnoreCase("Swimming") && x._10.matches("\\d+")})
      .map(x=>{(x._3,x._10.toInt)})
        .reduceByKey((x,y)=>{x+y})
        .sortBy(x=>{x._2})
        .collect()
    SwimmimgMedals.foreach(x=>{println(x)})
println("\n Inida Medals \n")
    val IndiaMedals=fileds.filter(x=>{x._3.equalsIgnoreCase("India")&& x._10.matches("\\d+")})
        .map(x=>{(x._4,x._10.toInt)})
        .reduceByKey((x,y)=>{x+y})
        .sortByKey()
        .collect()
    IndiaMedals.foreach(x=>{println(x)})

    println("\n Medals Per Country \n")

    val MedalsPerCountry=fileds.filter(x=>{x._10.matches("\\d+")})
        .map(x=>{(x._3,x._10.toInt)})
        .reduceByKey((x,y)=>{(x+y)})
        .sortByKey()
        .collect()

    MedalsPerCountry.foreach(x=>{println(x)})
    sc.stop()
  }
}
