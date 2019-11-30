import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{Partitioner, SparkContext}

object partitionByExample {

  def getCountryName(lines:String):(String,String)={
    val fileds=lines.split(",")

    return (fileds(2),fileds(0))
  }

  def getCharName(line:String):(Char,String)={
    val fileds=line.split(",")


    return (fileds(0).charAt(0),fileds(0))
  }

class ThreePartsPartitioner(  val numPartitions:Int) extends Partitioner{

   def getPartition(key: Any): Int =key match {
     case s:Char => {
       if (s.toUpper < 'J')
         {
           0
         }
       else if (s.toUpper >= 'J' && s.toUpper <= 'T')
         {
           1
         }
       else
         {
           2
         }
     }
   }
}

  def seqOp(data:(Array[Int],Int),Item:Int):(Array[Int],Int)={
      return (data._1:+Item,data._2+Item)
  }

  def comOp(d1:(Array[Int],Int),d2:(Array[Int],Int)):(Array[Int],Int)={
    return (d1._1.union(d2._1),d1._2+d2._2)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc=new SparkContext("local[*]","PartationByExample")

    val olympicData=sc.textFile("D:\\olympix_data.csv",3)

    val CountryName=olympicData.map(x=>{getCountryName(x)})

    val CharName=olympicData.filter(x=>{x.split(",")(0).length>0}) .map(x=>{getCharName(x)})

    val grpRDD=olympicData.glom().map(x=>{x.mkString(",")})

    println("\n GrpRDD \n")

    grpRDD.foreach(x=>{println(x)})

    println("\n CountryName \n")

    CountryName.foreach(x=>{println(x)})

    println("\n CharName \n")

    CharName.foreach(x=>{println(x)})

    println("\n Current CountryName Size %d \n".format(CountryName.partitions.size))

    println("\n Current CharName Size %d \n".format(CharName.partitions.size))

    val NewCharNamePartition=CharName.partitionBy(new Partitioner() {
      val numPartitions:Int= 2
      def getPartition(k:Any) = {
        if (k.asInstanceOf[Char].toUpper < 'H') {0} else {1}
      }
    })

    println("\n New CharName Size %d  \n".format(NewCharNamePartition.partitions.size))
    println("\n New CharNmae partationer %s\n".format(NewCharNamePartition.partitioner))

    val newCharNamePartitionList=NewCharNamePartition.glom().map(x=>{x.mkString(",")})

    newCharNamePartitionList.foreach(x=>{println(x)})

    val newCountryName=CountryName.partitionBy(new Partitioner {
      val numPartitions: Int = 4

       def getPartition(key: Any): Int = {
         val Cntry=key.asInstanceOf[String]

         if (Cntry.equalsIgnoreCase("india")||Cntry.equalsIgnoreCase("united states")||Cntry.equalsIgnoreCase("Australia"))
           {
             1
           }
         else if (Cntry.equalsIgnoreCase("Netherlands")||Cntry.equalsIgnoreCase("Russia") || Cntry.equalsIgnoreCase("Germany")||Cntry.equalsIgnoreCase("France"))
           {
             2
           }

         else if(Cntry.equalsIgnoreCase("Zimbabwe")||Cntry.equalsIgnoreCase("Jamaica")||Cntry.equalsIgnoreCase("South Africa")||Cntry.equalsIgnoreCase("Brazil"))
           {
             3
           }
         else
           {
             0
           }

       }
    })

   println("\n New newCountryName size %d \n".format(newCountryName.partitions.size))

    val newCountryNameVal=newCountryName.glom().map(x=>{x.mkString(",")})

   newCountryNameVal.foreach(x=>{println(x)})

    val newClassCharCountry=CharName.partitionBy(new ThreePartsPartitioner(3))

    println("\n Class Partiton Size %d \n".format(newClassCharCountry.partitions.size))

    val newClassCharCountryVal=newClassCharCountry.glom().map(x=>{x.mkString(",")})

    newClassCharCountryVal.foreach(x=>{println(x)})

    val CountCountry=CountryName.map(x=>{(x._1,1)}).reduceByKey((x,y)=>{(x+y)})
    val Key=CountCountry.map(x=>{x._2.toInt})
    val Value=CountCountry.map(x=>{x._1})

    val KevValue=Key.zip(Value)

    println("\n Key Value \n")

    KevValue.foreach(x=>{println(x)})

    val kevValuepart=KevValue.glom().map(x=>{x.mkString(",")})

    println("\n Key Value Part \n")

    kevValuepart.foreach(x=>{println(x)})

    val SumKey=Key.reduce((x,y)=>{x+y})

    println("\n Sum %d\n".format(SumKey))

val AggVal=Key.aggregate((Array[Int](),0))(seqOp,comOp)

    println("\n AggVal \n")

println(AggVal._1.mkString(","),AggVal._2)
  }

}
