import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object GroupBYKeys {

  def parseLine(line:String):(String,String,String,String)={
    val fields = line.split(",")

    return (fields(0),fields(1),fields(2),fields(3))
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sc=new SparkContext("local[*]","GroupBYKeys")

    val MovieData=sc.textFile("D:\\smallmoviereating.csv")

    val groupedData=MovieData.mapPartitionsWithIndex((idx,rec)=>{if(idx==1){rec.drop(1)}else{rec}})
      .map(x=>{parseLine(x)})
      .filter(x=>{x._1.matches("\\d+")})
      .map(x=>{(x._1.toInt,x._2)})
      .groupByKey().collectAsMap()

    groupedData.foreach(x=>{println(x)})

for (i <- groupedData.keys){
  for (k <- groupedData(i) )
  println(k)
}

   val GroupBy= MovieData.map(x=>{parseLine(x)._2}).groupBy(x=>{x.charAt(0)})

    GroupBy.foreach(x=>{println(x)})


  }
}
