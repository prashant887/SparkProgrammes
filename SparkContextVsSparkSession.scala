import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkContextVsSparkSession {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val config = new SparkConf()
      .setAppName("SparkContextVsSparkSession")
      .setMaster("local[*]")

    val sc = new SparkContext(config)
    val ss=SparkSession
      .builder()
      .config(config)
      .getOrCreate()


    val FileConentsSparkContext=sc.textFile("D:\\TitanicData.csv",4)
       // .mapPartitionsWithIndex((idx,rec)=>{if (idx==0) {rec.drop(1)}else{rec}}).collect()

    println("\n Reading Files Using SparkContext \n")

    FileConentsSparkContext.foreach(x=>{println(x)})

    println("Number of Parations %d".format(FileConentsSparkContext.partitions.size))
    val linesPerPartation=FileConentsSparkContext.mapPartitions(x=>{Array(x.size).iterator}).collect()
    println("lines per paration ")
    linesPerPartation.foreach(x=>{println(x)})

    val indexPartation=FileConentsSparkContext.mapPartitionsWithIndex((idx,iter)=>{Array(idx+","+iter.size).iterator}).collect()

    println("IndexPartation")
    indexPartation.foreach(x=>{println(x)})


    val FileConentsSparkSession=ss.read
        .option("header","true")
        .csv("D:\\TitanicData.csv")

    println("\n Reading Files Using SparkSession \n")

    FileConentsSparkSession.foreach(x=>{println(x(3),x(5))})

    sc.stop()
  }
}
