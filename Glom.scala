import org.apache.log4j.{Logger,Level}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.SparkSession
object Glom {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf=new SparkConf().setMaster("local[*]").setAppName("Glom")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    val sc=spark.sparkContext
    val range=(1 to 100).toList

    val rdd=sc.parallelize(range,4)

    println(rdd.getNumPartitions)
    println(rdd.partitioner)
    rdd.glom().foreach(x=>{println(x.mkString(","))})
    sc.stop()
    spark.stop()
  }
}
