import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

object accumulatorVariable {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("broadCasteVariable").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._
    val sc=spark.sparkContext
    val nameAge=spark.read.
      json("/Users/pl465j/Downloads/dataset-master/nameage.json")
    nameAge.show(truncate = false)


    val usPopulation=spark.read.format("csv").
      option("header","true").
      option("delimiter","|").
      load("/Users/pl465j/Downloads/dataset-master/uspopulation.csv")
    usPopulation.show(truncate = false)


    val counter=sc.longAccumulator("Counter")

    usPopulation.foreach(x=>{counter.add(1)})
    usPopulation.foreach(x=>{println(x)})

    println(counter,counter.value,counter.count)
    println(usPopulation.count())

    sc.stop()
    spark.stop()
    spark.close()



  }

  /*
 Accumlator is shared read variable , only driver can read
   */

}
