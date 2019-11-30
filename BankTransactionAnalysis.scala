import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BankTransactionAnalysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val config=new SparkConf()
      .setMaster("local[*]")
      .setAppName("BankTransactionAnalysis")

    val spark=SparkSession.builder()
      .config(config)
      .getOrCreate()

    val sc=spark.sparkContext

    val dataRdd=sc.textFile("C:\\Users\\PLaxmikant\\Downloads\\data_berka\\trans.asc")

    val transdata=dataRdd.map(x=>{x.split(";")})
        .filter(x=>{x(1).matches("\\d+")})
        .map(x=>{(x(1).toInt,(x(3).replaceAll("\"",""),x(5).toDouble))})
      .mapValues(x=>{
        if(x._1.equalsIgnoreCase("VYDAJ")){
          x._2 * -1
        }
        else {
          x._2
        }
      })
     // .filter(x=>{x._1==1})
       .reduceByKey((x,y)=>{x+y})
        .sortBy(x=>{x._1}).collect()

//    transdata.foreach(x=>{println(x)})

    val CreditDebitCount=dataRdd.map(x=>{x.split(";")})
      .filter(x=>{x(1).matches("\\d+")})
      .map(x=>{((x(1).toInt,x(3).replaceAll("\"","")),1)})
        .reduceByKey((x,y)=>{x+y})
      .map(x=>{(x._1._1,(x._1._2,x._2))})
      .groupByKey()
      .map(x=>{(x._1,x._2.toList)})
      .mapValues(x=>{
        if (x.length == 3)
          {
            (x(0)._1,x(0)._2,x(1)._1,x(1)._2,x(2)._1,x(2)._2)
          }
        else {
          (x(0)._1,x(0)._2,x(1)._1,x(1)._2)
        }
      })
        .sortByKey()
      //  .saveAsTextFile("D:\\BankTransactionOutput")


    println("\n CreditDebitCount \n")

   // CreditDebitCount.foreach(x=>{println(x)})

    val dataCountByKey=dataRdd.map(x=>{x.split(";")})
        .filter(x=>{x(1).matches("\\d+")})
        .map(x=>{(x(1).toInt,x(5).toDouble)})
        .countByKey()

    println("\n CountByKey \n")
    //dataCountByKey.foreach(x=>{println(x)})

    val dataCountByValue=dataRdd.map(x=>{x.split(";")})
      .filter(x=>{x(1).matches("\\d+")})
      .map(x=>{(x(1).toInt,x(5).toDouble)})
        .countByValue()

    println("\n Count By Value\n")

    //dataCountByValue.foreach(x=>{println(x)})

    val VYDAJRdd=dataRdd.map(x=>{x.split(";")})
      .filter(x=>{x(1).matches("\\d+")})
      .map(x=>{(x(1).toInt,(x(3).replaceAll("\"",""),x(5).toDouble))})
        .filter(x=>{x._2._1.equalsIgnoreCase("VYDAJ")})

    val PRIJEMRdd=dataRdd.map(x=>{x.split(";")})
      .filter(x=>{x(1).matches("\\d+")})
      .map(x=>{(x(1).toInt,(x(3).replaceAll("\"",""),x(5).toDouble))})
      .filter(x=>{x._2._1.equalsIgnoreCase("PRIJEM")})

    val FullJoin=VYDAJRdd.fullOuterJoin(PRIJEMRdd)
        .map(x=>{(x._1,x._2._1.getOrElse(0),x._2._2.getOrElse(0))})

    //FullJoin.foreach(x=>{println(x._1,x._2.toString,x._3.toString)})


    CreditDebitCount.lookup(1053).foreach(x=>{println(x)})
    sc.stop()
    spark.stop()
  }

}
