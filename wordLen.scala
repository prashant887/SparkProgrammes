import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.SparkConf
import org.apache.log4j.{Level,Logger}

object wordLen {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("dataSkewness").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    val sc=spark.sparkContext

    val rDD=sc.textFile("/Users/pl465j/Downloads/cisohacker")
      .flatMap(x=>{x.split(" ")})
      .map(x=>{(x,x.length)})
      .filter(x=>{x._2>6})

    rDD.collect().foreach(x=>{println(x)})

    sc.stop()
    spark.stop()
    spark.close()
  }
}
